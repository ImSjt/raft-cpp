// Copyright 2023 JT
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
#include "storage.h"

#include <algorithm>
#include <cassert>

#include "logger.h"
#include "util.h"

namespace craft {

MemoryStorage::MemoryStorage() {
  // When starting from scratch populate the list with a dummy entry at term
  // zero.
  raftpb::Entry entry;
  entry.set_index(0);
  entry.set_term(0);
  ents_.emplace_back(std::make_shared<raftpb::Entry>(std::move(entry)));
}

std::tuple<raftpb::HardState, raftpb::ConfState, Status>
MemoryStorage::InitialState() const {
  return std::make_tuple(hard_state_, snapshot_->metadata().conf_state(), Status::OK());
}

Status MemoryStorage::SetHardState(const raftpb::HardState& st) {
  std::lock_guard<std::shared_mutex> guard(mutex_);
  hard_state_ = st;
  return Status::OK();
}

std::tuple<EntryPtrs, Status>
MemoryStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size) {
  std::shared_lock<std::shared_mutex> guard(mutex_);

  assert(!ents_.empty());
  uint64_t offset = ents_[0]->index();
  if (lo <= offset) {
    return std::make_tuple(
        EntryPtrs(),
        Status::Error("%s [offset: %d, lo: %d]", kErrCompacted, offset, lo));
  }

  if (hi > LastIndexUnSafe() + 1) {
    // panic
    LOG_FATAL("entries' hi(%d) is out of bound lastindex(%d)", hi,
              LastIndexUnSafe());
  }

  // only contains dummy entries.
  if (ents_.size() == 1) {
    return std::make_tuple(
        EntryPtrs(),
        Status::Error("%s [len(ents_): %d]", kErrUnavailable, ents_.size()));
  }

  // copy entries[lo, hi)
  auto ents = EntryPtrs(&ents_[lo - offset], &ents_[hi - offset]);
  return std::make_tuple(Util::LimitSize(std::move(ents), max_size), Status::OK());
}

std::tuple<uint64_t, Status> MemoryStorage::Term(uint64_t i) {
  std::shared_lock<std::shared_mutex> guard(mutex_);

  assert(!ents_.empty());
  uint64_t offset = ents_[0]->index();
  if (i < offset) {
    return std::make_tuple(
        0, Status::Error("%s [offset: %d, i: %d]", kErrCompacted, offset, i));
  }
  // relative index
  uint64_t rel_index = i - offset;
  if (rel_index >= static_cast<uint64_t>(ents_.size())) {
    return std::make_tuple(
        0, Status::Error("%s [rel_index: %d, len(ents_): %d]", kErrUnavailable,
                         rel_index, ents_.size()));
  }
  return std::make_tuple(ents_[rel_index]->term(), Status::OK());
}

std::tuple<uint64_t, Status> MemoryStorage::LastIndex() {
  std::shared_lock<std::shared_mutex> guard(mutex_);
  return std::make_tuple(LastIndexUnSafe(), Status::OK());
}

std::tuple<uint64_t, Status> MemoryStorage::FirstIndex() {
  std::shared_lock<std::shared_mutex> guard(mutex_);
  return std::make_tuple(FirstIndexUnSafe(), Status::OK());
}

std::tuple<SnapshotPtr, Status> MemoryStorage::SnapShot() {
  std::shared_lock<std::shared_mutex> guard(mutex_);
  return std::make_tuple(snapshot_, Status::OK());
}

Status MemoryStorage::ApplySnapshot(SnapshotPtr snapshot) {
  std::lock_guard<std::shared_mutex> guard(mutex_);

	// handle check for old snapshot being applied
  uint64_t ms_index = snapshot_->metadata().index();
  uint64_t snap_index = snapshot->metadata().index();
  if (ms_index >= snap_index) {
    return Status::Error("%s [ms_index: %d, snap_index: %d]", kErrSnapOutOfDate,
                         ms_index, snap_index);
  }

  snapshot_ = snapshot;
  auto entry = std::make_shared<raftpb::Entry>();
  entry->set_term(snapshot->metadata().term());
  entry->set_index(snapshot->metadata().index());
  ents_.clear();
  ents_.emplace_back(std::make_shared<raftpb::Entry>());
  return Status::OK();
}

std::tuple<SnapshotPtr, Status> MemoryStorage::CreateSnapshot(
    uint64_t i, const raftpb::ConfState* cs, const std::string& data) {
  std::lock_guard<std::shared_mutex> guard(mutex_);

  if (i <= snapshot_->metadata().index()) {
    return std::make_tuple(
        SnapshotPtr(), Status::Error("%s [i: %d, index: %d]", kErrSnapOutOfDate,
                                     i, snapshot_->metadata().index()));
  }

  assert(!ents_.empty());
  uint64_t offset = ents_[0]->index();
  if (i > LastIndexUnSafe()) {
    // panic
    LOG_FATAL("snapshot %d is out of bound lastindex(%d)", i,
              LastIndexUnSafe());
  }

  snapshot_->mutable_metadata()->set_index(i);
  snapshot_->mutable_metadata()->set_term(ents_[i - offset]->term());
  if (cs != nullptr) {
    *(snapshot_->mutable_metadata()->mutable_conf_state()) = *cs;
  }

  *(snapshot_->mutable_data()) = data;
  return std::make_tuple(snapshot_, Status::OK());
}

Status MemoryStorage::Compact(uint64_t compact_index) {
  std::lock_guard<std::shared_mutex> guard(mutex_);

  assert(!ents_.empty());
  uint64_t offset = ents_[0]->index();
  if (compact_index <= offset) {
    return Status::Error("%s [compact_index: %d, offset: %d]", kErrCompacted,
                         compact_index, offset);
  }

  if (compact_index > LastIndexUnSafe()) {
    // panic
    LOG_FATAL("compact %d is out of bound lastindex(%d)", compact_index,
              LastIndexUnSafe());
  }

  // ents_[i] is the last entry in the snapshot.
  uint64_t i = compact_index - offset;
  ents_.erase(ents_.begin(), ents_.begin() + i);
  ents_.shrink_to_fit();

  return Status::OK();
}

Status MemoryStorage::Append(EntryPtrs entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  std::lock_guard<std::shared_mutex> guard(mutex_);

  uint64_t first = FirstIndexUnSafe();
  uint64_t last = entries[0]->index() + entries.size() - 1;

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
  if (first > entries[0]->index()) {
    entries.erase(entries.begin(),
                  entries.begin() + (first - entries[0]->index()));
  }

  uint64_t offset = entries[0]->index() - ents_[0]->index();
  uint64_t ents_size = static_cast<uint64_t>(ents_.size());
  if (offset < ents_size) {  // case 3
    ents_.erase(ents_.begin() + offset, ents_.end());
    ents_.insert(ents_.end(), entries.begin(), entries.end());
  } else if (offset == ents_size) {  // case4
    ents_.insert(ents_.end(), entries.begin(), entries.end());
  } else {  // case5
    // panic
    LOG_FATAL("missing log entry [last: %d, append at: %d]", LastIndexUnSafe(),
              entries[0]->index());
  }
  return Status::OK();
}

uint64_t MemoryStorage::LastIndexUnSafe() const {
  assert(!ents_.empty());
  return ents_[0]->index() + static_cast<uint64_t>(ents_.size()) - 1;
}

uint64_t MemoryStorage::FirstIndexUnSafe() const {
  assert(!ents_.empty());
  return ents_[0]->index() + 1;
}

}  // namespace craft