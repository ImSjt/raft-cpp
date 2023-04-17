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
#pragma once

#include <memory>
#include <vector>

#include "define.h"
#include "raftpb/raft.pb.h"
#include "status.h"

class RaftLog;
namespace craft {

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
class Unstable {
 public:
  Unstable() : offset_(0) {}

  // MaybeFirstIndex returns the index of the first possible entry in entries
  // if it has a snapshot.
  std::tuple<uint64_t, bool> MaybeFirstIndex() const;

  // MaybeLastIndex returns the last index if it has at least one
  // unstable entry or snapshot.
  std::tuple<uint64_t, bool> MaybeLastIndex() const;

  // MaybeTerm returns the term of the entry at index i, if there
  // is any.
  std::tuple<uint64_t, bool> MaybeTerm(uint64_t i) const;

  void StableTo(uint64_t i, uint64_t t);

  void StableSnapTo(uint64_t i);

  void Restore(SnapshotPtr snapshot);

  void TruncateAndAppend(const EntryPtrs& ents);

  EntryPtrs Slice(uint64_t lo, uint64_t hi) const;

  const EntryPtrs& Entries() const { return entries_; }

  SnapshotPtr Snapshot() const { return snapshot_; }

  uint64_t Offset() const { return offset_; }

  void SetSnapshot(SnapshotPtr snapshot) { snapshot_ = snapshot; }
  void SetEntries(const EntryPtrs& ents) { entries_ = ents; }
  void SetOffset(uint64_t offset) { offset_ = offset; }

 private:
  // shrinkEntriesArray discards the underlying array used by the entries slice
  // if most of it isn't being used. This avoids holding references to a bunch
  // of potentially large entries that aren't needed anymore. Simply clearing
  // the entries wouldn't be safe because clients might still be using them.
  void ShrikEntriesArray();

  // u.offset <= lo <= hi <= u.offset+len(u.entries)
  void MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

  friend class RaftLog;
  // the incoming unstable snapshot, if any.
  SnapshotPtr snapshot_;
  // all entries that have not yet been written to storage.
  EntryPtrs entries_;
  uint64_t offset_;
};

}  // namespace craft