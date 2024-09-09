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
#pragma once

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "src/raftpb/raft.pb.h"
#include "src/define.h"
#include "src/status.h"
#include "src/logger.h"

namespace craft {

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
const char* const kErrCompacted =
    "requested index is unavailable due to compaction";

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
const char* const kErrSnapOutOfDate =
    "requested index is older than the existing snapshot";

// ErrUnavailable is returned by Storage interface when the requested log
// entries are unavailable.
const char* const kErrUnavailable = "requested entry at index is unavailable";

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when
// the required snapshot is temporarily unavailable.
const char* const kErrSnapshotTemporarilyUnavailable =
    "snapshot is temporarily unavailable";

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
class Storage {
 public:
  virtual ~Storage() = default;

  // InitialState returns the saved HardState and ConfState information.
  virtual std::tuple<raftpb::HardState, raftpb::ConfState, Status>
  InitialState() const = 0;

  // Entries returns a slice of log entries in the range [lo,hi).
  // MaxSize limits the total size of the log entries returned, but
  // Entries returns at least one entry if any.
  virtual std::tuple<EntryPtrs, Status>
  Entries(uint64_t lo, uint64_t hi, uint64_t max_size) = 0;

  // Term returns the term of entry i, which must be in the range
  // [FirstIndex()-1, LastIndex()]. The term of the entry before
  // FirstIndex is retained for matching purposes even though the
  // rest of that entry may not be available.
  virtual std::tuple<uint64_t, Status> Term(uint64_t i) = 0;

  // LastIndex returns the index of the last entry in the log.
  virtual std::tuple<uint64_t, Status> LastIndex() = 0;

  // FirstIndex returns the index of the first log entry that is
  // possibly available via Entries (older entries have been incorporated
  // into the latest Snapshot; if storage only contains the dummy entry the
  // first log entry is not available).
  virtual std::tuple<uint64_t, Status> FirstIndex() = 0;

  // Snapshot returns the most recent snapshot.
  // If snapshot is temporarily unavailable, it should return
  // ErrSnapshotTemporarilyUnavailable, so raft state machine could know that
  // Storage needs some time to prepare snapshot and call Snapshot later.
  virtual std::tuple<std::shared_ptr<raftpb::Snapshot>, Status> SnapShot() = 0;
};

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class MemoryStorage : public Storage {
 public:
  MemoryStorage(std::shared_ptr<Logger> logger);

  // InitialState implements the Storage interface.
  std::tuple<raftpb::HardState, raftpb::ConfState, Status> InitialState()
      const override;

  // SetHardState saves the current HardState.
  Status SetHardState(const raftpb::HardState& st);

  // Entries implements the Storage interface.
  std::tuple<EntryPtrs, Status>
  Entries(uint64_t lo, uint64_t hi, uint64_t max_size) override;

  // Term implements the Storage interface.
  std::tuple<uint64_t, Status> Term(uint64_t i) override;

  // LastIndex implements the Storage interface.
  std::tuple<uint64_t, Status> LastIndex() override;

  // FirstIndex implements the Storage interface.
  std::tuple<uint64_t, Status> FirstIndex() override;

  // Snapshot implements the Storage interface.
  std::tuple<SnapshotPtr, Status> SnapShot() override;

  // ApplySnapshot overwrites the contents of this Storage object with
  // those of the given snapshot.
  Status ApplySnapshot(SnapshotPtr snapshot);

  // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
  // can be used to reconstruct the state at that point.
  // If any configuration changes have been made since the last compaction,
  // the result of the last ApplyConfChange must be passed in.
  std::tuple<SnapshotPtr, Status> CreateSnapshot(uint64_t i,
                                                 const raftpb::ConfState* cs,
                                                 const std::string& data);

  // Compact discards all log entries prior to compactIndex.
  // It is the application's responsibility to not attempt to compact an index
  // greater than raftLog.applied.
  Status Compact(uint64_t compact_index);

  // Append the new entries to storage.
  // TODO (xiangli): ensure the entries are continuous and
  // entries[0].Index > ms.entries[0].Index
  Status Append(EntryPtrs entries);

  void SetEntries(const EntryPtrs& ents) { ents_ = ents; }
  const EntryPtrs& GetEntries() const { return ents_; }
  EntryPtr GetEntry(size_t i) { return ents_[i]; }

  SnapshotPtr GetSnapshot() { return snapshot_; }

  void SetIgnoreSize(bool v) { ignore_size_ = v; }

 private:
  uint64_t LastIndexUnSafe() const;
  uint64_t FirstIndexUnSafe() const;

  std::shared_ptr<Logger> logger_;
  // Protects access to all fields. Most methods of MemoryStorage are
  // run on the raft thread, but Append() is run on an application
  // thread.
  std::shared_mutex mutex_;
  raftpb::HardState hard_state_;
  SnapshotPtr snapshot_;
  // ents[i] has raft log position i+snapshot.Metadata.Index
  EntryPtrs ents_;

  // for test
  bool ignore_size_;
};

}  // namespace craft