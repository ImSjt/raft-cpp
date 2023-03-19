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

#include <cstdint>
#include <memory>

#include "tracker/inflights.h"
#include "tracker/state.h"

namespace craft {

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
class Progress {
 public:
  Progress(uint64_t next, uint64_t match, int64_t max_inflight, bool is_learner, bool active);

  // ResetState moves the Progress into the specified State, resetting
  // ProbeSent, PendingSnapshot, and Inflights.
  void ResetState(StateType state);

  // ProbeAcked is called when this peer has accepted an append. It resets
  // ProbeSent to signal that additional append messages should be sent without
  // further delay.
  void ProbeAcked() { probe_sent_ = false; }

  // BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
  // optionally and if larger, the index of the pending snapshot.
  void BecomeProbe();

  // BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
  void BecomeReplicate();

  // BecomeSnapshot moves the Progress to StateSnapshot with the specified
  // pending snapshot index.
  void BecomeSnapshot(uint64_t snapshoti);

  // MaybeUpdate is called when an MsgAppResp arrives from the follower, with
  // the index acked by it. The method returns false if the given n index comes
  // from an outdated message. Otherwise it updates the progress and returns
  // true.
  bool MaybeUpdate(uint64_t n);

  // OptimisticUpdate signals that appends all the way up to and including index
  // n
  // are in-flight. As a result, Next is increased to n+1.
  void OptimisticUpdate(uint64_t n) { next_ = n + 1; }

  // MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
  // arguments are the index of the append message rejected by the follower, and
  // the hint that we want to decrease to.
  //
  // Rejections can happen spuriously as messages are sent out of order or
  // duplicated. In such cases, the rejection pertains to an index that the
  // Progress already knows were previously acknowledged, and false is returned
  // without changing the Progress.
  //
  // If the rejection is genuine, Next is lowered sensibly, and the Progress is
  // cleared for sending log entries.
  bool MaybeDecrTo(uint64_t rejected, uint64_t match_hint);

  // IsPaused returns whether sending log entries to this node has been
  // throttled. This is done when a node has rejected recent MsgApps, is
  // currently waiting for a snapshot, or has reached the MaxInflightMsgs limit.
  // In normal operation, this is false. A throttled node will be contacted less
  // frequently until it has reached a state in which it's able to accept a
  // steady stream of log entries again.
  bool IsPaused();

  uint64_t Match() const { return match_; }
  uint64_t Next() const { return next_; }
  StateType State() const { return state_; }
  uint64_t PendingSnapshot() const { return pending_snapshot_; }
  bool RecentActive() const { return recent_active_; }
  bool ProbeSent() const { return probe_sent_; }
  bool IsLearner() const { return is_learner_; }
  void SetIsLearner(bool v) { is_learner_ = v; }

  std::string String() const;

 private:
  uint64_t match_, next_;
  // state_ defines how the leader should interact with the follower.
  //
  // When in StateProbe, leader sends at most one replication message
  // per heartbeat interval. It also probes actual progress of the follower.
  //
  // When in StateReplicate, leader optimistically increases next
  // to the latest entry sent after sending replication message. This is
  // an optimized state for fast replicating log entries to the follower.
  //
  // When in StateSnapshot, leader should have sent out snapshot
  // before and stops sending any replication message.
  StateType state_;

  // pending_snapshot_ is used in StateSnapshot.
  // If there is a pending snapshot, the pending_snapshot_ will be set to the
  // index of the snapshot. If pending_snapshot_ is set, the replication process
  // of this Progress will be paused. raft will not resend snapshot until the
  // pending one is reported to be failed.
  uint64_t pending_snapshot_;

  // recent_active_ is true if the progress is recently active. Receiving any
  // messages from the corresponding follower indicates the progress is active.
  // recent_active_ can be reset to false after an election timeout.
  //
  // TODO(tbg): the leader should always have this set to true.
  bool recent_active_;

  // probe_sent_ is used while this follower is in StateProbe. When probe_sent_
  // is true, raft should pause sending replication message to this peer until
  // probe_sent_ is reset. See ProbeAcked() and IsPaused().
  bool probe_sent_;

  // inflights_ is a sliding window for the inflight messages.
  // Each inflight message contains one or more log entries.
  // The max number of entries per message is defined in raft config as
  // MaxSizePerMsg. Thus inflight effectively limits both the number of inflight
  // messages and the bandwidth each Progress can use. When inflights is Full,
  // no more message should be sent. When a leader sends out a message, the
  // index of the last entry should be added to inflights. The index MUST be
  // added into inflights in order. When a leader receives a reply, the previous
  // inflights should be freed by calling inflights.FreeLE with the index of the
  // last received entry.
  std::unique_ptr<Inflights> inflights_;

  // is_learner_ is true if this progress is tracked for a learner.
  bool is_learner_;
};

using ProgressPtr = std::shared_ptr<Progress>;

}  // namespace craft