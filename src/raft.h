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
#include <deque>
#include <memory>
#include <optional>

#include "log.h"
#include "quorum/quorum.h"
#include "read_only.h"
#include "storage.h"
#include "tracker/tracker.h"

namespace craft {

enum RaftStateType {
  kFollower,
  kCandidate,
  kLeader,
  kPreCandidate,
  kNumState
};

enum CampaignType {
  // kPreElection represents the first phase of a normal election when
  // Config.PreVote is true.
  kPreElection,
  // kElection represents a normal (time-based) election (the second phase
  // of the election when Config.PreVote is true).
  kElection,
  // kTransfer represents the type of leader transfer.
  kTransfer,
};

struct SoftState {
  uint64_t lead;
  RaftStateType raft_state;
};

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
struct Ready {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	std::optional<SoftState> soft_state;

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
  raftpb::HardState hard_state;

	// read_states can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
  std::deque<ReadState> read_states;

	// entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
  EntryPtrs entries;

	// snapshot specifies the snapshot to be saved to stable storage.
  SnapshotPtr snapshot;

	// committed_entries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
  EntryPtrs committed_entries;

	// messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
  MsgPtrs messages;

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	bool must_sync;

  // AppliedCursor extracts from the Ready the highest index the client has
  // applied (once the Ready is confirmed via Advance). If no information is
  // contained in the Ready, returns zero.
  uint64_t AppliedCursor() const;
};

const std::string& RaftStateTypeName(RaftStateType t);
const std::string& CampaignTypeName(CampaignType t);

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
const char* const kErrProposalDropped = "raft proposal dropped";

bool IsEmptyHardState(const raftpb::HardState& st);

class Raft {
 public:
  using TickFunc = std::function<void()>;
  using StepFunc = std::function<Status(MsgPtr m)>;

  static constexpr uint64_t kNone = 0;
  static constexpr uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();

  // Config contains the parameters to start a raft.
  struct Config {
    // id is the identity of the local raft. ID cannot be 0.
    uint64_t id = kNone;

    // election_tick is the number of Node.Tick invocations that must pass
    // between elections. That is, if a follower does not receive any message
    // from the leader of current term before election_tick_ has elapsed, it
    // will become candidate and start an election. election_tick_ must be
    // greater than hearbeat_tick_. We suggest election_tick_ = 10 *
    // hearbeat_tick_ to avoid unnecessary leader switching.
    int64_t election_tick = 0;

    // heartbeat_tick is the number of Node.Tick invocations that must pass
    // between heartbeats. That is, a leader sends heartbeat messages to
    // maintain its leadership every hearbeat_tick_ ticks.
    int64_t heartbeat_tick = 0;

    // storage is the storage for raft. raft generates entries and states to be
    // stored in storage. raft reads the persisted entries and states out of
    // storage when it needs. raft reads out the previous state and
    // configuration out of storage when restarting.
    std::shared_ptr<Storage> storage;

    // applied is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to
    // applied. If applied is unset when restarting, raft might return
    // previous applied entries. This is a very application dependent
    // configuration.
    uint64_t applied = 0;

    // max_size_per_msg limits the max byte size of each append message.
    // Smaller value lowers the raft recovery cost(initial probing and message
    // lost during normal operation). On the other side, it might affect the
    // throughput during normal replication. Note: math.MaxUint64 for unlimited,
    // 0 for at most one entry per message.
    uint64_t max_size_per_msg = 0;

    // max_committed_size_per_ready limits the size of the committed entries
    // which can be applied.
    uint64_t max_committed_size_per_ready = 0;

    // max_uncommitted_entries_size limits the aggregate byte size of the
    // uncommitted entries that may be appended to a leader's log. Once this
    // limit is exceeded, proposals will begin to return ErrProposalDropped
    // errors. Note: 0 for no limit.
    uint64_t max_uncommitted_entries_size = 0;

    // max_inflight_msgs limits the max number of in-flight append messages
    // during optimistic replication phase. The application transportation layer
    // usually has its own sending buffer over TCP/UDP. Setting
    // max_inflight_msgs to avoid overflowing that sending buffer. TODO
    // (xiangli): feedback to application to limit the proposal rate?
    int64_t max_inflight_msgs = 0;

    // check_quorum specifies if the leader should check quorum activity.
    // Leader steps down when quorum is not active for an electionTimeout.
    bool check_quorum = false;

    // pre_vote enables the Pre-Vote algorithm described in raft thesis section
    // 9.6. This prevents disruption when a node that has been partitioned away
    // rejoins the cluster.
    bool pre_vote = false;

    // read_only_option specifies how the read only request is processed.
    //
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    //
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than
    // it should (clock can move backward/pause without any bound). ReadIndex is
    // not safe in that case. CheckQuorum MUST be enabled if read_only_option
    // is ReadOnlyLeaseBased.
    ReadOnly::ReadOnlyOption read_only_option = ReadOnly::ReadOnlyOption::kSafe;

    // disable_proposal_forwarding set to true means that followers will drop
    // proposals, rather than forwarding them to the leader. One use case for
    // this feature would be in a situation where the Raft leader is used to
    // compute the data of a proposal, for example, adding a timestamp from a
    // hybrid logical clock to data in a monotonically increasing way.
    // Forwarding should be disabled to prevent a follower with an inaccurate
    // hybrid logical clock from assigning the timestamp and then forwarding the
    // data to the leader.
    bool disable_proposal_forwarding = false;

    Status Validate();
  };

  static std::unique_ptr<Raft> New(Config& c);

  Raft(const Config& c, std::unique_ptr<RaftLog>&& raft_log);

  bool HasLeader() const { return lead_ != kNone; }

  SoftState GetSoftState() const {
    return SoftState{.lead = lead_, .raft_state = state_};
  }

  raftpb::HardState GetHardState() const {
    raftpb::HardState hard_state;
    hard_state.set_term(term_);
    hard_state.set_vote(vote_);
    hard_state.set_commit(raft_log_->Committed());
    return hard_state;
  }

  // send schedules persisting state to a stable storage and AFTER that
  // sending the message (as part of next Ready message processing).
  void Send(MsgPtr m);

  // SendAppend sends an append RPC with new entries (if any) and the
  // current commit index to the given peer.
  void SendAppend(uint64_t to);

  // MaybeSendAppend sends an append RPC with new entries to the given peer,
  // if necessary. Returns true if a message was sent. The sendIfEmpty
  // argument controls whether messages with no entries will be sent
  // ("empty" messages are useful to convey updated Commit indexes, but
  // are undesirable when we're sending multiple messages in a batch).
  bool MaybeSendAppend(uint64_t to, bool send_if_empty);

  // SendHeartbeat sends a heartbeat RPC to the given peer.
  void SendHeartbeat(uint64_t to, const std::string& ctx);

  // BcastAppend sends RPC, with entries to all peers that are not up-to-date
  // according to the progress recorded in trk_.
  void BcastAppend();

  // bcastHeartbeat sends RPC, without entries to all the peers.
  void BcastHeartbeat();

  void BcastHeartbeatWithCtx(const std::string& ctx);

  void Advance(const Ready& rd);

  bool MaybeCommit();

  void Reset(uint64_t term);

  bool AppendEntry(const EntryPtr& e);
  bool AppendEntry(const EntryPtrs& es);

  void Tick() { tick_(); }
  void TickQuiesced() { election_elapsed_++; }

  // TickElection is run by followers and candidates after election_timeout_.
  void TickElection();

  // TickHeartbeat is run by leaders to send a MsgBeat after heartbeat_timeout_.
  void TickHeartbeat();

  void BecomeFollower(uint64_t term, uint64_t lead);

  void BecomeCandidate();

  void BecomePreCandidate();

  void BecomLeader();

  void Hup(CampaignType t);

  void Campaign(CampaignType t);

  std::tuple<int64_t, int64_t, VoteState> Poll(uint64_t id,
                                               raftpb::MessageType t, bool v);

  Status Step(MsgPtr m);

  Status StepLeader(MsgPtr m);

  Status StepCandidate(MsgPtr m);

  Status StepFollower(MsgPtr m);

  void HandleAppendEntries(MsgPtr m);

  void HandleHearbeat(MsgPtr m);

  void HandleSnapshot(MsgPtr m);

  // Restore recovers the state machine from a snapshot. It restores the log and
  // the configuration of state machine. If this method returns false, the
  // snapshot was ignored, either because it was obsolete or because of an
  // error.
  bool Restore(raftpb::Snapshot* s);

  // Promotable indicates whether state machine can be promoted to leader,
  // which is true when its own id is in progress list.
  bool Promotable();

  raftpb::ConfState ApplyConfChange(raftpb::ConfChangeV2&& cc);

  // SwitchToConfig reconfigures this node to use the provided configuration. It
  // updates the in-memory state and, when necessary, carries out additional
  // actions such as reacting to the removal of nodes or changed quorum
  // requirements.
  //
  // The inputs usually result from restoring a ConfState or applying a
  // ConfChange.
  raftpb::ConfState SwitchToConfig(const ProgressTracker::Config& cfg,
                                    const ProgressMap& prs);

  void LoadState(const raftpb::HardState& state);

  // PastElectionTimeout returns true iff r.electionElapsed is greater
  // than or equal to the randomized election timeout in
  // [electiontimeout, 2 * electiontimeout - 1].
  bool PastElectionTimeout() const;

  void ResetRandomizedElectionTimeout();

  void SendTimeoutNow(uint64_t to);

  void AbortLeaderTransfer() { lead_transferee_ = kNone; }

  // CommittedEntryinCurrentTerm return true if the peer has committed an entry
  // in its term.
  bool CommittedEntryinCurrentTerm();

  // ResponseToReadIndexReq constructs a response for `req`. If `req` comes from
  // the peer itself, a blank value will be returned.
  MsgPtr ResponseToReadIndexReq(MsgPtr req, uint64_t read_index);

  // IncreaseUncommittedSize computes the size of the proposed entries and
  // determines whether they would push leader over its maxUncommittedSize
  // limit. If the new entries would exceed the limit, the method returns false.
  // If not, the increase in uncommitted entry size is recorded and the method
  // returns true.
  //
  // Empty payloads are never refused. This is used both for appending an empty
  // entry at a new leader's term, as well as leaving a joint configuration.
  bool IncreaseUncommittedSize(const EntryPtrs& ents);

  // ReduceUncommittedSize accounts for the newly committed entries by
  // decreasing the uncommitted entry size limit.
  void ReduceUncommittedSize(const EntryPtrs& ents);

  int64_t NumOfPendingConf(const EntryPtrs& ents);

  void ReleasePendingReadIndexMessages();

  void SendMsgReadIndexResponse(MsgPtr m);

  uint64_t Term() const { return term_; }

  const ProgressTracker& GetTracker() const { return trk_; }
  ProgressTracker& GetTracker() { return trk_; }
  const RaftLog* GetRaftLog() const { return raft_log_.get(); }

  uint64_t ID() const { return id_; }

  std::deque<MsgPtr> Msgs() const { return msgs_; }

  const std::deque<ReadState>& GetReadStates() const { return read_states_; }
  void ClearReadStates() { read_states_.clear(); }

  void ClearMsgs() { msgs_.clear(); }

 private:
  uint64_t id_;

  uint64_t term_;
  uint64_t vote_;

  std::deque<ReadState> read_states_;

  std::unique_ptr<RaftLog> raft_log_;

  uint64_t max_msg_size_;
  uint64_t max_uncommitted_size_;

  ProgressTracker trk_;

  RaftStateType state_;

  // is_learner_ is true if the local raft node is a learner.
  bool is_learner_;

  std::deque<MsgPtr> msgs_;

  // the leader id
  uint64_t lead_;
  // lead_transferee_ is id of the leader transfer target when its value is not
  // zero. Follow the procedure defined in raft thesis 3.10.
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

  // number of ticks since it reached last heartbeat_elapsed_.
  // only leader keeps heartbeat_elapsed_.
  int64_t heartbeat_elapsed_;

  bool check_quorum_;
  bool pre_vote_;

  int64_t heartbeat_timeout_;
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