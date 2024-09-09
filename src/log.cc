// Copyright 2023 juntaosu
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "src/log.h"

#include <cassert>
#include <cstring>

#include "src/logger.h"
#include "src/util.h"

namespace craft {

static bool IsEmptySnap(SnapshotPtr snapshot) {
  assert(snapshot != nullptr);
  return snapshot->metadata().index() == 0;
}

RaftLog::RaftLog(std::shared_ptr<Logger> logger,
                 std::shared_ptr<Storage> storage,
                 uint64_t max_next_ents_size)
    : logger_(logger),
      storage_(storage),
      unstable_(logger),
      committed_(0),
      applied_(0),
      max_next_ents_size_(max_next_ents_size) {
  assert(storage_ != nullptr);

  auto [first_index, s1] = storage->FirstIndex();
  assert(s1.IsOK());
  auto [last_index, s2] = storage->LastIndex();
  assert(s2.IsOK());

  unstable_.offset_ = last_index + 1;
  // Initialize our committed and applied pointers to the time of the last
  // compaction.
  committed_ = first_index - 1;
  applied_ = first_index - 1;
}

std::tuple<uint64_t, bool> RaftLog::MaybeAppend(uint64_t index,
                                                uint64_t log_term,
                                                uint64_t committed,
                                                EntryPtrs ents) {
  if (MatchTerm(index, log_term)) {
    uint64_t lastnewi = index + static_cast<uint64_t>(ents.size());
    uint64_t ci = FindConflict(ents);
    if (ci == 0) {
      // do nothing
    } else if (ci <= committed_) {
      CRAFT_LOG_FATAL(logger_, "entry %d conflict with committed entry [committed(%d)]", ci,
                committed_);
    } else {
      uint64_t offset = index + 1;
      if (ci - offset > static_cast<uint64_t>(ents.size())) {
        CRAFT_LOG_FATAL(logger_, "index, %d, is out of range [%d]", ci - offset, ents.size());
      }
      ents.erase(ents.begin(), ents.begin() + (ci - offset));
      Append(ents);
    }
    CommitTo(std::min(committed, lastnewi));
    return std::make_tuple(lastnewi, true);
  }
  return std::make_tuple(0, false);
}

EntryPtrs RaftLog::UnstableEntries() const {
  if (unstable_.entries_.empty()) {
    return EntryPtrs();
  }
  return unstable_.Entries();
}

EntryPtrs RaftLog::NextEnts() const {
  uint64_t off = std::max(applied_ + 1, FirstIndex());

  if (committed_ + 1 > off) {
    auto [ents, status] = Slice(off, committed_ + 1, max_next_ents_size_);
    if (!status.IsOK()) {
      // panic
      CRAFT_LOG_FATAL(logger_, "unexpected error when getting unapplied entries (%s)",
                status.Str());
    }
    return ents;
  }
  return EntryPtrs();
}

bool RaftLog::HasNextEnts() const {
  uint64_t off = std::max(applied_ + 1, FirstIndex());
  return committed_ + 1 > off;
}

bool RaftLog::HasPendingSnapshot() const {
  return unstable_.Snapshot() != nullptr &&
         !IsEmptySnap(unstable_.Snapshot());
}

std::tuple<SnapshotPtr, Status> RaftLog::Snapshot() const {
  if (unstable_.Snapshot() != nullptr) {
    return std::make_tuple(unstable_.Snapshot(), Status::OK());
  }
  return storage_->SnapShot();
}

uint64_t RaftLog::FirstIndex() const {
  auto [index1, ok] = unstable_.MaybeFirstIndex();
  if (ok) {
    return index1;
  }
  auto [index2, status] = storage_->FirstIndex();
  assert(status.IsOK());
  return index2;
}

uint64_t RaftLog::LastIndex() const {
  auto [index1, ok] = unstable_.MaybeLastIndex();
  if (ok) {
    return index1;
  }
  auto [index2, status] = storage_->LastIndex();
  assert(status.IsOK());
  return index2;
}

void RaftLog::CommitTo(uint64_t tocommit) {
  // never decrease commit
  if (committed_ < tocommit) {
    if (LastIndex() < tocommit) {
      CRAFT_LOG_FATAL(logger_,
          "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log "
          "corrupted, truncated, or lost?",
          tocommit, LastIndex());
    }
    committed_ = tocommit;
  }
}

void RaftLog::AppliedTo(uint64_t i) {
  if (i == 0) {
    return;
  }
  if (committed_ < i || i < applied_) {
    CRAFT_LOG_FATAL(logger_, "applied(%d) is out of range [prevApplied(%d), committed(%d)]", i,
              applied_, committed_);
  }
  applied_ = i;
}

uint64_t RaftLog::LastTerm() const {
  auto [term, status] = Term(LastIndex());
  if (!status.IsOK()) {
    CRAFT_LOG_FATAL(logger_, "unexpected error when getting the last term (%s)", status.Str());
  }
  return term;
}

std::tuple<uint64_t, Status> RaftLog::Term(uint64_t i) const {
  // the valid term range is [index of dummy entry, last index]
  uint64_t dummy_index = FirstIndex() - 1;
  if (i < dummy_index || i > LastIndex()) {
		// TODO: return an error instead?
    return std::make_tuple(0, Status::OK());
  }

  auto [term1, ok] = unstable_.MaybeTerm(i);
  if (ok) {
    return std::make_tuple(term1, Status::OK());
  }

  auto [term2, status] = storage_->Term(i);
  if (status.IsOK()) {
    return std::make_tuple(term2, Status::OK());
  }

  if (std::strstr(status.Str(), kErrCompacted) ||
      std::strstr(status.Str(), kErrUnavailable)) {
    return std::make_tuple(0, std::move(status));
  }

  CRAFT_LOG_FATAL(logger_, "unexpected error: %s", status.Str());
  return std::make_tuple(0, std::move(status));
}

std::tuple<EntryPtrs, Status> RaftLog::Entries(uint64_t i, uint64_t maxsize) const {
  if (i > LastIndex()) {
    return std::make_tuple(EntryPtrs(), Status::OK());
  }
  return Slice(i, LastIndex() + 1, maxsize);
}

EntryPtrs RaftLog::AllEntries() const {
  auto [ents, status] = Entries(FirstIndex(), kNoLimit);
  if (status.IsOK()) {
    return ents;
  }
  if (std::strstr(status.Str(), kErrCompacted)) {
    // try again if there was a racing compaction
    return AllEntries();
  }
  CRAFT_LOG_FATAL(logger_, "unexpected error: %s", status.Str());
  return EntryPtrs();
}

bool RaftLog::IsUpToData(uint64_t lasti, uint64_t term) const {
  return term > LastTerm() || (term == LastTerm() && lasti >= LastIndex());
}

bool RaftLog::MatchTerm(uint64_t i, uint64_t term) const {
  auto [t, status] = Term(i);
  if (!status.IsOK()) {
    return false;
  }
  return t == term;
}

bool RaftLog::MaybeCommit(uint64_t max_index, uint64_t term) {
  if (max_index > committed_ && ZeroTermOnErrCompacted(Term(max_index)) == term) {
    CommitTo(max_index);
    return true;
  }
  return false;
}

void RaftLog::Restore(SnapshotPtr snapshot) {
  CRAFT_LOG_INFO(logger_, "starts to restore snapshot [index: %d, term: %d]",
           snapshot->metadata().index(), snapshot->metadata().term());
  committed_ = snapshot->metadata().index();
  unstable_.Restore(snapshot);
}

uint64_t RaftLog::Append(const EntryPtrs& ents) {
  if (ents.empty()) {
    return LastIndex();
  }

  uint64_t after = ents[0]->index() - 1;
  if (after < committed_) {
    CRAFT_LOG_FATAL(logger_, "after(%d) is out of range [committed(%d)]", after, committed_);
  }

  unstable_.TruncateAndAppend(ents);

  return LastIndex();
}

uint64_t RaftLog::FindConflict(const EntryPtrs& ents) const {
  for (auto& ent : ents) {
    if (!MatchTerm(ent->index(), ent->term())) {
      if (ent->index() <= LastIndex()) {
        CRAFT_LOG_INFO(logger_,
            "found conflict at index %d [existing term: %d, conflicting term: "
            "%d]",
            ent->index(), ZeroTermOnErrCompacted(Term(ent->index())),
            ent->term());
      }
      return ent->index();
    }
  }
  return 0;
}

uint64_t RaftLog::FindConflictByTerm(uint64_t index, uint64_t term) {
  auto li = LastIndex();
  if (index > li) {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
    CRAFT_LOG_WARNING(logger_, "index(%llu) is out of range [0, lastIndex(%llu)] in findConflictByTerm",
      index, li);
    return index;
  }
  while (1) {
    auto [log_term, status] = Term(index);
    if (log_term <= term || !status.IsOK()) {
      break;
    }
    index--;
  }
  return index;
}

std::tuple<EntryPtrs, Status> RaftLog::Slice(uint64_t lo, uint64_t hi,
                                             uint64_t max_size) const {
  Status status = MustCheckOutOfBounds(lo, hi);
  if (!status.IsOK()) {
    return std::make_tuple(EntryPtrs(), std::move(status));
  }
  if (lo == hi) {
    return std::make_tuple(EntryPtrs(), Status::OK());
  }
  EntryPtrs ents;
  if (lo < unstable_.offset_) {
    assert(storage_ != nullptr);
    auto [stored_ents, status] =
        storage_->Entries(lo, std::min(hi, unstable_.Offset()), max_size);
    if (!status.IsOK()) {
      if (std::strstr(status.Str(), kErrCompacted)) {
        return std::make_tuple(EntryPtrs(), std::move(status));
      } else if (std::strstr(status.Str(), kErrUnavailable)) {
        CRAFT_LOG_FATAL(logger_, "entries[%d:%d) is unavailable from storage", lo,
                  std::min(hi, unstable_.Offset()));
      } else {
        CRAFT_LOG_FATAL(logger_, "unexpected error: %s", status.Str());
      }
    }

    // check if ents has reached the size limitation
    if (static_cast<uint64_t>(stored_ents.size()) <
        std::min(hi, unstable_.Offset()) - lo) {
      return std::make_tuple(std::move(stored_ents), Status::OK());
    }
    ents = std::move(stored_ents);
  }

  if (hi > unstable_.Offset()) {
    auto unstable = unstable_.Slice(std::max(lo, unstable_.Offset()), hi);
    ents.insert(ents.end(), unstable.begin(), unstable.end());
  }
  return std::make_tuple(Util::LimitSize(std::move(ents), max_size), Status::OK());
}

Status RaftLog::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
  if (lo > hi) {
    CRAFT_LOG_FATAL(logger_, "invalid slice %d > %d", lo, hi);
  }
  uint64_t fi = FirstIndex();
  if (lo < fi) {
    return Status::Error(kErrCompacted);
  }
  uint64_t len = LastIndex() + 1 - fi;
  if (hi > fi + len) {
    CRAFT_LOG_FATAL(logger_, "slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, LastIndex());
  }
  return Status::OK();
}

uint64_t RaftLog::ZeroTermOnErrCompacted(std::tuple<uint64_t, Status> t) const {
  auto& [term, status] = t;
  if (status.IsOK()) {
    return term;
  }
  if (std::strstr(status.Str(), kErrCompacted)) {
    return 0;
  }
  CRAFT_LOG_FATAL(logger_, "unexpected error: %s", status.Str());
  return 0;
}

}  // namespace craft
