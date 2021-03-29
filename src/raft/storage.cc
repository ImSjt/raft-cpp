#include "craft/storage.h"

#include <cassert>
#include <algorithm>

#include "common/logger.h"

namespace craft {

MemoryStorage::MemoryStorage() {
    // When starting from scratch populate the list with a dummy entry at term zero.
    raftpb::Entry entry;
    entry.set_index(0);
    entry.set_term(0);
    ents_.push_back(entry);
}

Status MemoryStorage::InitialState(raftpb::HardState& hard_state, raftpb::ConfState& conf_state) const {
    hard_state = hard_state_;
    conf_state = snapshot_.metadata().conf_state();
    return Status::OK();
}

Status MemoryStorage::SetHardState(const raftpb::HardState& st) {
    std::lock_guard<std::mutex> guard(mutex_);
    hard_state_ = st;
    return Status::OK();
}

Status MemoryStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size, std::vector<raftpb::Entry>& ents) {
    std::lock_guard<std::mutex> guard(mutex_);
    
    assert(!ents_.empty());

    uint64_t offset = ents_[0].index();
    if (lo <= offset) {
        return Status::Error("%s [offset: %d, lo: %d]", kErrCompacted, offset, lo);
    }

    if (hi > LastIndexUnSafe()+1) {
        // panic
        LOG_FATAL("entries' hi(%d) is out of bound lastindex(%d)", hi, LastIndexUnSafe());
    }

    if (ents_.size() == 1) {
        return Status::Error("%s [len(ents_): %d]", kErrUnavailable, ents_.size());
    }

    // copy entries[lo, hi)
    ents = std::vector<raftpb::Entry>(&ents_[lo-offset], &ents_[hi-offset]);
    LimitSize(ents, max_size);

    return Status::OK();
}

Status MemoryStorage::Term(uint64_t i, uint64_t& term) {    
    std::lock_guard<std::mutex> guard(mutex_);

    assert(!ents_.empty());

    term = 0;

    uint64_t offset = ents_[0].index();
    if (i < offset) {
        return Status::Error("%s [offset: %d, i: %d]", kErrCompacted, offset, i);
    }

    // relative index
    uint64_t rel_index = i-offset;
    if (rel_index >= static_cast<uint64_t>(ents_.size())) {
        return Status::Error("%s [rel_index: %d, len(ents_): %d]", kErrUnavailable, rel_index, ents_.size());
    }

    term = ents_[rel_index].term();

    return Status::OK();
}

uint64_t MemoryStorage::LastIndex() {
    std::lock_guard<std::mutex> guard(mutex_);
    return LastIndexUnSafe();
}

uint64_t MemoryStorage::FirstIndex() {
    std::lock_guard<std::mutex> guard(mutex_);
    return FirstIndexUnSafe();
}

Status MemoryStorage::SnapShot(raftpb::Snapshot& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    snapshot_ = snapshot;
    return Status::OK();
}

Status MemoryStorage::ApplySnapshot(const raftpb::Snapshot& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t ms_index = snapshot_.metadata().index();

    const raftpb::SnapshotMetadata& metadata = snapshot.metadata();
    uint64_t snap_index = metadata.index();

    if (ms_index >= snap_index) {
        return Status::Error("%s [ms_index: %d, snap_index: %d]", kErrSnapOutOfDate, ms_index, snap_index);
    }

    snapshot_ = snapshot;

    raftpb::Entry entry;
    entry.set_term(metadata.term());
    entry.set_index(metadata.index());

    ents_.clear();
    ents_.push_back(entry);

    return Status::OK();
}

Status MemoryStorage::CreateSnapshot(uint64_t i, const raftpb::ConfState* cs, const std::string& data, raftpb::Snapshot& snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (i <= snapshot_.metadata().index()) {
        return Status::Error("%s [i: %d, index: %d]", kErrSnapOutOfDate, i, snapshot_.metadata().index());
    }

    assert(!ents_.empty());
    uint64_t offset = ents_[0].index();
    if (i > LastIndexUnSafe()) {
        // panic
        LOG_FATAL("snapshot %d is out of bound lastindex(%d)", i, LastIndexUnSafe());
    }

    snapshot_.mutable_metadata()->set_index(i);
    snapshot_.mutable_metadata()->set_term(ents_[i-offset].term());
    if (cs != nullptr) {
        *(snapshot_.mutable_metadata()->mutable_conf_state()) = *cs;
    }

    *(snapshot_.mutable_data()) = data;

    snapshot = snapshot_;

    return Status::OK();
}

Status MemoryStorage::Compact(uint64_t compact_index) {
    std::lock_guard<std::mutex> guard(mutex_);

    assert(!ents_.empty());
    uint64_t offset = ents_[0].index();
    if (compact_index <= offset) {
        return Status::Error("%s [compact_index: %d, offset: %d]", kErrCompacted, compact_index, offset);
    }

    if (compact_index > LastIndexUnSafe()) {
        // panic
        LOG_FATAL("compact %d is out of bound lastindex(%d)", compact_index, LastIndexUnSafe());
    }

    // ents_[i] is the last entry in the snapshot.
    uint64_t i = compact_index - offset;
    ents_.erase(ents_.begin(), ents_.begin()+i);
    ents_.shrink_to_fit();

    return Status::OK();
}

Status MemoryStorage::Append(std::vector<raftpb::Entry>& entries) {
    if (entries.empty()) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> guard(mutex_);

    uint64_t first = FirstIndexUnSafe();
    uint64_t last = entries[0].index() + entries.size() - 1;

    //          [  snapshot  ][       ents_       ]
    // case1:      [entries]
    // case2:             [entries]
    // case3:                       [entries]
    // case4:                                     [entries]
    // case5:                                           [entries]

    // case1
    // shortcut if there is no new entry.
    if (last < first) {
        return Status::OK();
    }

    // case2
    // truncate compacted entries.
    if (first > entries[0].index()) {
        entries.erase(entries.begin(), entries.begin()+(first-entries[0].index()));
    }

    uint64_t offset = entries[0].index() - ents_[0].index();
    uint64_t ents_size = static_cast<uint64_t>(ents_.size());
    if (offset < ents_size) { // case 3
        ents_.erase(ents_.begin()+offset, ents_.end());
        ents_.insert(ents_.end(), entries.begin(), entries.end());
    } else if(offset == ents_size) { // case4
        ents_.insert(ents_.end(), entries.begin(), entries.end());
    } else { // case5
        // panic
        LOG_FATAL("missing log entry [last: %d, append at: %d]", LastIndexUnSafe(), entries[0].index());
    }

    return Status::OK();
}

uint64_t MemoryStorage::LastIndexUnSafe() const {
    assert(!ents_.empty());
    return ents_[0].index() + static_cast<uint64_t>(ents_.size()) - 1;
}

uint64_t MemoryStorage::FirstIndexUnSafe() const {
    assert(!ents_.empty());
    return ents_[0].index() + 1;
}

void MemoryStorage::LimitSize(std::vector<raftpb::Entry>& ents, uint64_t max_size) const {
    if (ents.empty()) {
        return;
    }

    uint64_t size = 0;
    for (auto it = ents.begin(); it != ents.end(); ++it) {
        size += static_cast<uint64_t>(it->ByteSizeLong());
        if (size > max_size) {
            ents.erase(it, ents.end());
            break;
        }
    }
}

} // namespace craft