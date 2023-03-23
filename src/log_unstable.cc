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
#include "log_unstable.h"

#include "logger.h"

namespace craft {

std::tuple<uint64_t, bool> Unstable::MaybeFirstIndex() const {
  if (snapshot_ != nullptr) {
    return std::make_tuple(snapshot_->metadata().index() + 1, true);
  }
  return std::make_tuple(0, false);
}

std::tuple<uint64_t, bool> Unstable::MaybeLastIndex() const {
  if (!entries_.empty()) {
    return std::make_tuple(offset_ + static_cast<uint64_t>(entries_.size()) - 1, true);
  }

  if (snapshot_ != nullptr) {
    return std::make_tuple(snapshot_->metadata().index(), true);
  }
  return std::make_tuple(0, false);
}

std::tuple<uint64_t, bool> Unstable::MaybeTerm(uint64_t i) const {
  if (i < offset_) {
    if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
      return std::make_tuple(snapshot_->metadata().term(), true);
    }
    return std::make_tuple(0, false);
  }

  auto [last, ok] = MaybeLastIndex();
  if (!ok) {
    return std::make_tuple(0, false);
  }
  if (i > last) {
    return std::make_tuple(0, false);
  }

  return std::make_tuple(entries_[i - offset_]->term(), true);
}

void Unstable::StableTo(uint64_t i, uint64_t t) {
  auto [gt, ok] = MaybeTerm(i);
  if (!ok) {
    return;
  }

  // if i < offset, term is matched with the snapshot
  // only update the unstable entries if term is matched with
  // an unstable entry.
  if (gt == t && i >= offset_) {
    entries_.erase(entries_.begin(), entries_.begin() + (i - offset_ + 1));
    offset_ = i + 1;
    ShrikEntriesArray();
  }
}

void Unstable::StableSnapTo(uint64_t i) {
  if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
    snapshot_ = nullptr;
  }
}

void Unstable::Restore(SnapshotPtr snapshot) {
  offset_ = snapshot->metadata().index() + 1;
  entries_.clear();
  snapshot = snapshot;
}

void Unstable::TruncateAndAppend(const EntryPtrs& ents) {
  uint64_t after = ents[0]->index();
  if (after == offset_ + entries_.size()) {
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
    // MustCheckOutOfBounds(offset_, after);
    entries_.erase(entries_.begin() + (after - offset_), entries_.end());
    entries_.insert(entries_.end(), ents.begin(), ents.end());
  }
}

EntryPtrs Unstable::Slice(uint64_t lo, uint64_t hi) const {
  MustCheckOutOfBounds(lo, hi);
  std::vector<std::shared_ptr<raftpb::Entry>> ents;
  ents.insert(ents.end(), entries_.begin() + (lo - offset_),
              entries_.begin() + (hi - offset_));
  return ents;
}

void Unstable::ShrikEntriesArray() { entries_.shrink_to_fit(); }

void Unstable::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
  // assert(lo <= hi);
  // uint64_t upper = offset_ + static_cast<uint64_t>(entries_.size());
  // assert(lo >= offset_);
  // assert(hi <= upper);

  if (lo > hi) {
    LOG_FATAL("invalid unstable.slice %d > %d", lo, hi);
  }

  uint64_t upper = offset_ + static_cast<uint64_t>(entries_.size());
  if (lo < offset_ || hi > upper) {
    LOG_FATAL("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, offset_,
              upper);
  }
}

}  // namespace craft