#include "craft/log_unstable.h"

#include "common/logger.h"

namespace craft {

uint64_t Unstable::MaybeFirstIndex() const {
    if (snapshot_ != nullptr) {
        return snapshot_->metadata().index() + 1;
    }

    return 0;
}

uint64_t Unstable::MaybeLastIndex() const {
    if (!entries_.empty()) {
        return offset_ + static_cast<uint64_t>(entries_.size()) - 1;
    }

    if (snapshot_ != nullptr) {
        return snapshot_->metadata().index();
    }

    return 0;
}

uint64_t Unstable::MaybeTerm(uint64_t i) const {
    if (i < offset_) {
        if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
            return snapshot_->metadata().term();
        }

        return 0;
    }

    uint64_t last = MaybeLastIndex();
    if (last == 0) {
        return 0;
    }

    if (i > last) {
        return 0;
    }

    return entries_[i-offset_].term();
}

void Unstable::StableTo(uint64_t i, uint64_t t) {
    uint64_t gt = MaybeTerm(i);
    if (gt == 0) {
        return;
    }

    // if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
    if (gt == t && i >= offset_) {
        entries_.erase(entries_.begin(), entries_.begin()+(i-offset_+1));
        offset_ = i + 1;
        ShrikEntriesArray();
    }
}

void Unstable::StableSnapTo(uint64_t i) {
    if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
        snapshot_ = nullptr;
    }
}

void Unstable::Restore(const raftpb::Snapshot& snapshot) {
    offset_ = snapshot.metadata().index() + 1;
    entries_.clear();
    snapshot_.reset(new raftpb::Snapshot(snapshot));
}

void Unstable::TruncateAndAppend(const std::vector<raftpb::Entry>& ents) {
    assert(!ents.empty());
    uint64_t after = ents[0].index();
    if (after == offset_+entries_.size()) {
        // after is the next index in the u.entries
		// directly append
        entries_.insert(entries_.end(), ents.begin(), ents.end());
    } else if (after <= offset_) {
        LOG_INFO("replace the unstable entries from index %d", after);
        // The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
        offset_ = after;
        entries_ = ents;
    } else {
        // truncate to after and copy to u.entries
		// then append
        MustCheckOutOfBounds(offset_, after);
        entries_.erase(entries_.begin()+(after-offset_), entries_.end());
        entries_.insert(entries_.end(), ents.begin(), ents.end());
    }
}

void Unstable::Slice(uint64_t lo, uint64_t hi, std::vector<raftpb::Entry>& ents) const {
    MustCheckOutOfBounds(lo, hi);
    ents.insert(ents.end(), entries_.begin()+(lo-offset_), entries_.begin()+(hi-offset_));
}

void Unstable::ShrikEntriesArray() {
    entries_.shrink_to_fit();
}

void Unstable::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
    if (lo > hi) {
        LOG_FATAL("invalid unstable.slice %d > %d", lo, hi);
    }

    uint64_t upper = offset_ + static_cast<uint64_t>(entries_.size());
    if (lo < offset_ || hi > upper) {
        LOG_FATAL("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, offset_, upper);
    }
}

} // namespace craft