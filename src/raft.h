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
#include <deque>

#include "read_only.h"
#include "storage.h"
#include "tracker/tracker.h"

namespace craft {

using MsgPtr = std::shared_ptr<raftpb::Message>;

enum StateType { kFollower, kCandidate, kLeader, kPreCandidate, kNumState };

struct SoftState {
  uint64_t lead;
  StateType raft_state;
};

class Raft {
 public:
  using TickFunc = std::function<void ()>;
  using StepFunc = std::function<Status (MsgPtr m)>;

  static const uint64_t kNone = 0;


  // Config contains the parameters to start a raft.
  struct Config {
    // ID is the identity of the local raft. ID cannot be 0.
    uint64_t id_;

    // election_tick_ is the number of Node.Tick invocations that must pass between
    // elections. That is, if a follower does not receive any message from the
    // leader of current term before election_tick_ has elapsed, it will become
    // candidate and start an election. election_tick_ must be greater than
    // hearbeat_tick_. We suggest election_tick_ = 10 * hearbeat_tick_ to avoid
    // unnecessary leader switching.
    int64_t election_tick_;

    // hearbeat_tick_ is the number of Node.Tick invocations that must pass between
    // heartbeats. That is, a leader sends heartbeat messages to maintain its
    // leadership every hearbeat_tick_ ticks.
    int64_t hearbeat_tick_;

    // storage_ is the storage for raft. raft generates entries and states to be
    // stored in storage. raft reads the persisted entries and states out of
    // storage_ when it needs. raft reads out the previous state and configuration
    // out of storage when restarting.
    std::shared_ptr<Storage> storage_;

    // applied_ is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to
    // applied_. If applied_ is unset when restarting, raft might return previous
    // applied entries. This is a very application dependent configuration.
    uint64_t applied_;

    // max_size_per_msg_ limits the max byte size of each append message. Smaller
    // value lowers the raft recovery cost(initial probing and message lost
    // during normal operation). On the other side, it might affect the
    // throughput during normal replication. Note: math.MaxUint64 for unlimited,
    // 0 for at most one entry per message.
    uint64_t max_size_per_msg_;

    // max_committed_size_per_ready_ limits the size of the committed entries which
    // can be applied.
    uint64_t max_committed_size_per_ready_;

    // max_uncommitted_entries_size_ limits the aggregate byte size of the
    // uncommitted entries that may be appended to a leader's log. Once this
    // limit is exceeded, proposals will begin to return ErrProposalDropped
    // errors. Note: 0 for no limit.
    uint64_t max_uncommitted_entries_size_;

    // max_inflight_msgs_ limits the max number of in-flight append messages during
    // optimistic replication phase. The application transportation layer usually
    // has its own sending buffer over TCP/UDP. Setting max_inflight_msgs_ to avoid
    // overflowing that sending buffer. TODO (xiangli): feedback to application to
    // limit the proposal rate?
    int64_t max_inflight_msgs_;

    // check_quorum_ specifies if the leader should check quorum activity. Leader
    // steps down when quorum is not active for an electionTimeout.
    bool check_quorum_;

    // pre_vote_ enables the Pre-Vote algorithm described in raft thesis section
    // 9.6. This prevents disruption when a node that has been partitioned away
    // rejoins the cluster.
    bool pre_vote_;

    // read_only_option_ specifies how the read only request is processed.
    //
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    //
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    // CheckQuorum MUST be enabled if read_only_option_ is ReadOnlyLeaseBased.
    ReadOnly::ReadOnlyOption read_only_option_;

    // disable_proposal_forwarding_ set to true means that followers will drop
    // proposals, rather than forwarding them to the leader. One use case for
    // this feature would be in a situation where the Raft leader is used to
    // compute the data of a proposal, for example, adding a timestamp from a
    // hybrid logical clock to data in a monotonically increasing way. Forwarding
    // should be disabled to prevent a follower with an inaccurate hybrid
    // logical clock from assigning the timestamp and then forwarding the data
    // to the leader.
    bool disable_proposal_forwarding_;

    Status Validate() const;
  };

  std::unique_ptr<Raft> New(const Config& c);

  bool HasLeader() const { return lead_ != kNone; }

  SoftState GetSoftState() const { return SoftState{.lead = lead_, .raft_state = state_ }; }

  raftpb::HardState GetHardState() const;

 private:
  uint64_t id_;

  uint64_t term_;
  uint64_t vote_;

  std::deque<ReadState> read_states_;

  uint64_t max_msg_size_;
  uint64_t max_uncommitted_size_;

  ProgressTracker trk_;

  StateType state_;

	// is_learner_ is true if the local raft node is a learner.
  bool is_learner_;

  std::deque<MsgPtr> msgs;

  // the leader id
  uint64_t lead_;
	// lead_transferee_ is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
  uint64_t lead_transferee_;

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
  uint64_t pending_conf_index_;
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
  uint64_t uncommitted_size_;

  std::unique_ptr<ReadOnly> read_only_;

	// number of ticks since it reached last election_timeout_ when it is leader
	// or candidate.
	// number of ticks since it reached last election_timeout_ or received a
	// valid message from current leader when it is a follower.
  int64_t election_elapsed_;

	// number of ticks since it reached last hearbeat_timeout_.
	// only leader keeps heartbeatElapsed.
  int64_t hearbeat_elapsed_;

  bool check_quorum_;
  bool pre_vote_;

  int64_t hearbeat_timeout_;
  int64_t election_timeout_;
	// randomized_election_timeout_ is a random number between
	// [election_timeout_, 2 * election_timeout_ - 1]. It gets reset
	// when raft changes its state to follower or candidate.
  int64_t randomized_election_timeout_;
  bool disable_proposal_forwarding_;

  TickFunc tick_;
  StepFunc step_;

	// pending_read_index_messages_ is used to store messages of type MsgReadIndex
	// that can't be answered as new leader didn't committed any log in
	// current term. Those will be handled as fast as first log is committed in
	// current term.
  std::deque<MsgPtr> pending_read_index_messages_;
};

}  // namespace craft