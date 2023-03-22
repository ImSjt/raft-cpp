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
#include <string>

#include "craft/define.h"
#include "craft/log_unstable.h"
#include "craft/status.h"
#include "craft/storage.h"

namespace craft {

class RaftLog {
 public:
  static const uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();

  // New returns log using the given storage and default options. It
  // recovers the log to the state that it just commits and applies the
  // latest snapshot.
  static std::unique_ptr<RaftLog> New(std::shared_ptr<Storage>& storage) {
    return NewWithSize(storage, kNoLimit);
  }

  // NewWithSize returns a log using the given storage and max
  // message size.
  static std::unique_ptr<RaftLog> NewWithSize(std::shared_ptr<Storage> storage,
                                    uint64_t max_next_ents_size) {
    return std::make_unique<RaftLog>(storage, max_next_ents_size);
  }

  RaftLog(std::shared_ptr<Storage> storage,
          uint64_t max_next_ents_size = kNoLimit);

  std::string String() const {
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "committed=%d, applied=%d, unstable.offset=%d, "
             "len(unstable.Entries)=%d",
             committed_, applied_, unstable_.offset_,
             unstable_.entries_.size());
    return std::string(buf);
  }

  // MaybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
  // it returns (last index of new entries, true).
  std::tuple<uint64_t, bool> MaybeAppend(uint64_t index, uint64_t log_term,
                                         uint64_t committed,
                                         EntryPtrs ents);

  EntryPtrs UnstableEntries() const;

  // NextEnts returns all the available entries for execution.
  // If applied is smaller than the index of snapshot, it returns all committed
  // entries after the index of snapshot.
  EntryPtrs NextEnts() const;

  // HasNextEnts returns if there is any available entries for execution. This
  // is a fast check without heavy raftLog.Slice() in raftLog.NextEnts().
  bool HasNextEnts() const;

  // HasPendingSnapshot returns if there is pending snapshot waiting for
  // applying.
  bool HasPendingSnapshot() const;

  std::tuple<SnapshotPtr, Status> Snapshot() const;

  uint64_t FirstIndex() const;

  uint64_t LastIndex() const;

  void CommitTo(uint64_t tocommit);  // private?

  void AppliedTo(uint64_t i);

  void StableTo(uint64_t i, uint64_t t) { unstable_.StableTo(i, t); }

  void StableSnapTo(uint64_t i) { unstable_.StableSnapTo(i); }

  uint64_t LastTerm() const;

  std::tuple<uint64_t, Status> Term(uint64_t i) const;

  std::tuple<EntryPtrs, Status> Entries(uint64_t i, uint64_t maxsize) const;

  // AllEntries returns all entries in the log.
  EntryPtrs AllEntries() const;

  // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
  // by comparing the index and term of the last entries in the existing logs.
  // If the logs have last entries with different terms, then the log with the
  // later term is more up-to-date. If the logs end with the same term, then
  // whichever log has the larger lastIndex is more up-to-date. If the logs are
  // the same, the given log is up-to-date.
  bool IsUpToData(uint64_t lasti, uint64_t term) const;

  bool MatchTerm(uint64_t i, uint64_t term) const;

  bool MaybeCommit(uint64_t max_index, uint64_t term);

  void Restore(SnapshotPtr snapshot);

 private:
  uint64_t Append(const EntryPtrs& ents);

  // FindConflict finds the index of the conflict.
  // It returns the first pair of conflicting entries between the existing
  // entries and the given entries, if there are any.
  // If there is no conflicting entries, and the existing entries contains
  // all the given entries, zero will be returned.
  // If there is no conflicting entries, but the given entries contains new
  // entries, the index of the first new entry will be returned.
  // An entry is considered to be conflicting if it has the same index but
  // a different term.
  // The first entry MUST have an index equal to the argument 'from'.
  // The index of the given entries MUST be continuously increasing.
  uint64_t FindConflict(const EntryPtrs& ents) const;

  // FindConflictByTerm takes an (index, term) pair (indicating a conflicting log
  // entry on a leader/follower during an append) and finds the largest index in
  // log l with a term <= `term` and an index <= `index`. If no such index exists
  // in the log, the log's first index is returned.
  //
  // The index provided MUST be equal to or less than l.lastIndex(). Invalid
  // inputs log a warning and the input index is returned.
  uint64_t FindConflictByTerm(uint64_t index, uint64_t term);

  // Slice returns a slice of log entries from lo through hi-1, inclusive.
  std::tuple<EntryPtrs, Status> Slice(uint64_t lo, uint64_t hi, uint64_t max_size) const;

  // firstIndex <= lo <= hi <= firstIndex + len(l.entries)
  Status MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

  uint64_t ZeroTermOnErrCompacted(std::tuple<uint64_t, Status> t) const;

  // storage_ contains all stable entries since the last snapshot.
  std::shared_ptr<Storage> storage_;
  // unstable_ contains all unstable entries and snapshot.
  // they will be saved into storage.
  Unstable unstable_;
  // committed_ is the highest log position that is known to be in
  // stable storage on a quorum of nodes.
  uint64_t committed_;
  // applied_ is the highest log position that the application has
  // been instructed to apply to its state machine.
  // Invariant: applied_ <= committed_
  uint64_t applied_;
  // max_next_ents_size_ is the maximum number aggregate byte size of the
  // messages returned from calls to NextEnts.
  uint64_t max_next_ents_size_;
};

}  // namespace craft