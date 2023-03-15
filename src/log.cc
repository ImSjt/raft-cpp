#include "craft/log.h"

#include <cassert>

#include "common/logger.h"
#include "common/util.h"

namespace craft {

static bool IsEmptySnap(raftpb::Snapshot* snapshot) {
    assert(snapshot != nullptr);
    return snapshot->metadata().index() == 0;
}

RaftLog::RaftLog(std::shared_ptr<Storage>& storage, uint64_t max_next_ents_size)
    : storage_(storage),
      committed_(0),
      applied_(0),
      max_next_ents_size_(max_next_ents_size) {
    assert(storage_ != nullptr);

    uint64_t first_index = storage->FirstIndex();

    uint64_t last_index = storage->LastIndex();

    unstable_.offset_ = last_index + 1;

	// Initialize our committed and applied pointers to the time of the last compaction.
    committed_ = first_index - 1;
    applied_ = first_index - 1;
}

uint64_t RaftLog::MaybeAppend(uint64_t index, uint64_t log_term, uint64_t committed, std::vector<raftpb::Entry>& ents) {
    if (MatchTerm(index, log_term)) {
        uint64_t lastnewi = index + static_cast<uint64_t>(ents.size());
        uint64_t ci = FindConflict(ents);
        if (ci == 0) {
            // do nothing
        } else if (ci <= committed_) {
            LOG_FATAL("entry %d conflict with committed entry [committed(%d)]", ci, committed_);
        } else {
            uint64_t offset = index + 1;
            ents.erase(ents.begin(), ents.begin()+(ci-offset));
            Append(ents);
        }
        CommitTo(std::min(committed, lastnewi));

        return lastnewi;
    }

    return 0;
}

void RaftLog::UnstableEntries(std::vector<raftpb::Entry>& ents) const {
    if (unstable_.entries_.empty()) {
        return;
    }

    ents = unstable_.entries_;
}

void RaftLog::NextEnts(std::vector<raftpb::Entry>& ents) const {
    uint64_t off = std::max(applied_+1, FirstIndex());

    if (committed_+1 > off) {
        Status status = Slice(off, committed_+1, max_next_ents_size_, ents);
        if (!status.ok()) {
            // panic
            LOG_FATAL("unexpected error when getting unapplied entries (%s)", status.Str());
        }
    }
}

bool RaftLog::HasNextEnts() const {
    uint64_t off = std::max(applied_+1, FirstIndex());
    return committed_+1 > off;
}

bool RaftLog::HasPendingSnapshot() const {
    return unstable_.snapshot_ != nullptr && IsEmptySnap(unstable_.snapshot_.get());
}

Status RaftLog::Snapshot(raftpb::Snapshot& snapshot) const {
    if (unstable_.snapshot_ != nullptr) {
        snapshot = *(unstable_.snapshot_);
        return Status::OK();
    }

    return storage_->SnapShot(snapshot);
}

uint64_t RaftLog::FirstIndex() const {
    uint64_t index = unstable_.MaybeFirstIndex();
    if (index != 0) {
        return index;
    }

    index = storage_->FirstIndex();
    return index;
}

uint64_t RaftLog::LastIndex() const {
    uint64_t index = unstable_.MaybeLastIndex();
    if (index != 0) {
        return index;
    }

    index = storage_->LastIndex();
    return index;
}

void RaftLog::CommitTo(uint64_t tocommit) {
    // never decrease commit
    if (committed_ < tocommit) {
        if (LastIndex() < tocommit) {
            LOG_FATAL("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, LastIndex());
        }
        committed_ = tocommit;
    }
}

void RaftLog::AppliedTo(uint64_t i) {
    if (i == 0) {
        return;
    }

    if (committed_ < i || i < applied_) {
        LOG_FATAL("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, applied_, committed_);
    }

    applied_ = i;
}

uint64_t RaftLog::LastTerm() const {
    uint64_t term;
    Status status = Term(LastIndex(), term);
    if (!status.ok()) {
        LOG_FATAL("unexpected error when getting the last term (%s)", status.Str());
    }

    return term;
}

Status RaftLog::Term(uint64_t i, uint64_t& term) const {
    term = 0;
    // the valid term range is [index of dummy entry, last index]
    uint64_t dummy_index = FirstIndex() - 1;
    if (i < dummy_index || i > LastIndex()) {
        return Status::OK();
    }

    term = unstable_.MaybeTerm(i);
    if (term != 0) {
        return Status::OK();
    }

    Status status = storage_->Term(i, term);
    if (status.ok()) {
        return Status::OK();
    }

    if (std::strstr(status.Str(), kErrCompacted) ||
        std::strstr(status.Str(), kErrUnavailable)) {
        return status;
    }

    LOG_FATAL("unexpected error: %s", status.Str());
    return status;
}

Status RaftLog::Entries(uint64_t i, uint64_t maxsize, std::vector<raftpb::Entry>& ents) const {
    if (i > LastIndex()) {
        return Status::OK();
    }

    return Slice(i, LastIndex()+1, maxsize, ents);
}

void RaftLog::AllEntries(std::vector<raftpb::Entry>& ents) const {
    Status status = Entries(FirstIndex(), std::numeric_limits<uint64_t>::max(), ents);
    if (status.ok()) {
        return;
    }

    if (std::strstr(status.Str(), kErrCompacted)) {
        // try again if there was a racing compaction
        return AllEntries(ents);
    }

    LOG_FATAL("unexpected error: %s", status.Str());
}

bool RaftLog::IsUpToData(uint64_t lasti, uint64_t term) const {
    return term > LastTerm() || (term == LastTerm() && lasti >= LastIndex());
}

bool RaftLog::MatchTerm(uint64_t i, uint64_t term) const {
    uint64_t t;
    Status status = Term(i, t);
    if (!status.ok()) {
        return false;
    }

    return t == term;
}

bool RaftLog::MaybeCommit(uint64_t max_index, uint64_t term) {
    uint64_t t;
    Status status = Term(max_index, t);
    if (max_index > committed_ && ZeroTermOnErrCompacted(t, status) == term) {
        CommitTo(max_index);
        return true;
    }

    return false;
}

void RaftLog::Restore(const raftpb::Snapshot& snapshot) {
    LOG_INFO("starts to restore snapshot [index: %d, term: %d]", snapshot.metadata().index(), snapshot.metadata().term());
    committed_ = snapshot.metadata().index();
    unstable_.Restore(snapshot);
}

uint64_t RaftLog::Append(const std::vector<raftpb::Entry>& ents) {
    if (ents.empty()) {
        return LastIndex();
    }

    uint64_t after = ents[0].index() - 1;
    if (after < committed_) {
        LOG_FATAL("after(%d) is out of range [committed(%d)]", after, committed_);
    }

    unstable_.TruncateAndAppend(ents);

    return LastIndex();
}

uint64_t RaftLog::FindConflict(const std::vector<raftpb::Entry>& ents) const {
    for (auto& ent : ents) {
        if (!MatchTerm(ent.index(), ent.term())) {
            if (ent.index() < LastIndex()) {
                uint64_t term;
                Status status = Term(ent.index(), term);
                LOG_INFO("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ent.index(), ZeroTermOnErrCompacted(term, status), ent.term());
            }
            return ent.index();
        }
    }

    return 0;
}

Status RaftLog::Slice(uint64_t lo, uint64_t hi, uint64_t max_size, std::vector<raftpb::Entry>& ents) const {
    Status status = MustCheckOutOfBounds(lo, hi);
    if (!status.ok()) {
        return status;
    }

    if (lo == hi) {
        return Status::OK();
    }

    if (lo < unstable_.offset_) {
        assert(storage_ != nullptr);
        status = storage_->Entries(lo, std::min(hi, unstable_.offset_), max_size, ents);
        if (!status.ok()) {
            if (std::strstr(status.Str(), kErrCompacted)) {
                return status;
            } else if (std::strstr(status.Str(), kErrUnavailable)) {
                LOG_FATAL("entries[%d:%d) is unavailable from storage", lo, std::min(hi, unstable_.offset_));
            } else {
                LOG_FATAL("unexpected error: %s", status.Str());
            }
        }

        // check if ents has reached the size limitation
        if (static_cast<uint64_t>(ents.size()) < std::min(hi, unstable_.offset_)-lo) {
            return Status::OK();
        }
    }

    if (hi > unstable_.offset_) {
        unstable_.Slice(std::max(lo, unstable_.offset_), hi, ents);
    }

    Util::LimitSize(ents, max_size);

    return Status::OK();
}

Status RaftLog::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
    if  (lo > hi) {
        LOG_FATAL("invalid slice %d > %d", lo, hi);
    }

    uint64_t fi = FirstIndex();
    if (lo < fi) {
        return Status::Error("%s [lo: %d, fi: %d]", kErrCompacted, lo, fi);
    }

    uint64_t len = LastIndex() + 1 - fi;
    if (hi > fi+len) {
        LOG_FATAL("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, LastIndex());
    }

    return Status::OK();
}

uint64_t RaftLog::ZeroTermOnErrCompacted(uint64_t t, const Status& status) const {
    if (status.ok()) {
        return t;
    }

    if (std::strstr(status.Str(), kErrCompacted)) {
        return 0;
    }

    LOG_FATAL("unexpected error: %s", status.Str());

    return 0;
}

} //namespace craft
