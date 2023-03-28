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
#include "raft.h"

#include "confchange/restore.h"
#include "logger.h"
#include "util.h"
#include "raftpb/confstate.h"

namespace craft {

const std::string CampaignType::kCampaignPreElection = "CampaignPreElection";
const std::string CampaignType::kCampaignElection = "CampaignElection";
const std::string CampaignType::kCampaignTransfer = "CampaignTransfer";
const std::string CampaignType::kCampaignUnknow = "CampaignUnknow";

bool IsEmptyHardState(const raftpb::HardState& st) {
  return !st.has_term() && !st.has_vote() && !st.has_commit();
}

Status Raft::Config::Validate() {
  if (id == kNone) {
    return Status::Error("cannot use none as id");
  }

  if (heartbeat_tick <= 0) {
    return Status::Error("heartbeat tick must be greater than 0");
  }

  if (election_tick <= heartbeat_tick) {
    return Status::Error("election tick must be greater than heartbeat tick");
  }

  if (!storage) {
    return Status::Error("storage cannot be null");
  }

  if (max_uncommitted_entries_size == 0) {
    max_uncommitted_entries_size = kNoLimit;
  }

  // default MaxCommittedSizePerReady to MaxSizePerMsg because they were
  // previously the same parameter.
  if (max_committed_size_per_ready == 0) {
    max_committed_size_per_ready = max_size_per_msg;
  }

  if (max_inflight_msgs <= 0) {
    return Status::Error("max inflight messafes must be greater than 0");
  }

  if (read_only_option == ReadOnly::ReadOnlyOption::kLeaseBased &&
      !check_quorum) {
    return Status::Error(
        "check_quorum_ must be enabled when read_only_options_ is kLeaseBased");
  }

  return Status::OK();
}

std::unique_ptr<Raft> Raft::New(Raft::Config& c) {
  auto s1 = c.Validate();
  if (!s1.IsOK()) {
    LOG_FATAL("config is invalied, err: %s", s1.Str());
  }

  auto raft_log =
      RaftLog::NewWithSize(c.storage, c.max_committed_size_per_ready);
  auto [hs, cs, s2] = c.storage->InitialState();
  if (!s2.IsOK()) {
    LOG_FATAL("state is invalied, err:%s", s2.Str());
  }

  if (c.applied > 0) {
    raft_log->AppliedTo(c.applied);
  }

  auto r = std::make_unique<Raft>(c, std::move(raft_log));

  auto [cfg, prs, s3] =
      ::craft::Restore(Changer(r->GetTracker(), raft_log->LastIndex()), cs);
  if (!s3.IsOK()) {
    LOG_FATAL("restore error, err:%s", s3.Str());
  }

  auto cs2 = r->SwitchToConfig(cfg, prs);
  assert(cs == cs2);

  if (!IsEmptyHardState(hs)) {
    r->LoadState(hs);
  }

  r->BecomeFollower(r->Term(), kNone);

  return std::move(r);
}

Raft::Raft(const Config& c, std::unique_ptr<RaftLog>&& raft_log)
    : id_(c.id),
      lead_(kNone),
      is_learner_(false),
      raft_log_(std::move(raft_log)),
      max_msg_size_(c.max_size_per_msg),
      max_uncommitted_size_(c.max_uncommitted_entries_size),
      trk_(c.max_inflight_msgs),
      election_timeout_(c.election_tick),
      heartbeat_timeout_(c.heartbeat_tick),
      check_quorum_(c.check_quorum),
      pre_vote_(c.pre_vote),
      read_only_(std::make_unique<ReadOnly>(c.read_only_option)),
      disable_proposal_forwarding_(c.disable_proposal_forwarding) {}

raftpb::HardState Raft::GetHardState() const {}

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
// according to the progress recorded in Raft.prs.
void BcastAppend();

// bcastHeartbeat sends RPC, without entries to all the peers.
void BcastHeartbeat();

void BcastHeartbeatWithCtx(const std::string& ctx);

// void Advance(const Ready& rd);

bool MaybeCommit();

void Raft::Reset(uint64_t term) {
  if (term_ != term) {
    term_ = term;
    vote_ = kNone;
  }
  lead_ = kNone;

  election_elapsed_ = 0;
  hearbeat_elapsed_ = 0;
  ResetRandomizedElectionTimeout();

  AbortLeaderTransfer();

  trk_.ResetVotes();
  trk_.Visit([this](uint64_t id, ProgressPtr& pr) {
    pr =
        std::make_shared<Progress>(GetRaftLog()->LastIndex() + 1, 0,
                                   GetTracker().MaxInflight(), pr->IsLearner());
    if (id == ID()) {
      pr->SetMatch(GetRaftLog()->LastIndex());
    }
  });

  pending_conf_index_ = 0;
  uncommitted_size_ = 0;
  read_only_ = std::make_unique<ReadOnly>(read_only_->GetReadOnlyOption());
}

bool AppendEntry(const EntryPtrs& es);

// TickElection is run by followers and candidates after election_timeout_.
void Raft::TickElection() {
  election_elapsed_++;
  if (Promotable() && PastElectionTimeout()) {
    election_elapsed_ = 0;
    auto m = std::make_shared<raftpb::Message>();
    m->set_from(id_);
    m->set_type(raftpb::MessageType::MsgHup);
    auto status = Step(m);
    if (!status.IsOK()) {
      LOG_DEBUG("error occurred during election: %s", status.Str());
    }
  }
}

// TickHearbeat is run by leaders to send a MsgBeat after heartbeat_timeout_.
void TickHearbeat();

void Raft::BecomeFollower(uint64_t term, uint64_t lead) {
  step_ = std::bind(&Raft::StepFollower, this, std::placeholders::_1);
  Reset(term);
  tick_ = std::bind(&Raft::TickElection, this);
  lead_ = lead;
  state_ = RaftStateType::kFollower;
  LOG_INFO("%d became follower at term %d", id_, term_);
}

void BecomeCandidate();

void BecomePreCandidate();

void BecomLeader();

void Hup(CampaignType t);

void Campaign(CampaignType t);

std::tuple<int64_t, int64_t, VoteState> Poll(uint64_t id, raftpb::MessageType t,
                                             bool v);

Status Raft::Step(MsgPtr m) {
  // Handle the message term, which may result in our stepping down to a
  // follower.
  if (m->term() == 0) {
    // local message
  } else if (m->term() > term_) {
    if (m->type() == raftpb::MessageType::MsgVote ||
        m->type() == raftpb::MessageType::MsgPreVote) {
      bool force = m->context() == CampaignType(CampaignType::kTransfer).String();
      bool is_lease = check_quorum_ && lead_ != kNone && election_elapsed_ < election_timeout_;
      if (!force && is_lease) {
        // If a server receives a RequestVote request within the minimum
        // election timeout of hearing from a current leader, it does not update
        // its term or grant its vote
        LOG_INFO(
            "%llu [logterm: %llu, index: %llu, vote: %llu] ignored %s from "
            "%llu [logterm: %llu, index: %llu] at term %llu: lease is not "
            "expired (remaining ticks: %lld)",
            id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_,
            raftpb::MessageType_Name(m->type()).c_str(), m->from(), m->term(),
            m->index(), term_, election_timeout_ - election_elapsed_);
        return Status::OK();
      }
      if (m->type() == raftpb::MessageType::MsgPreVote) {
        // Never change our term in response to a PreVote
      } else if (m->type() == raftpb::MessageType::MsgPreVoteResp && !m->reject()) {
        // We send pre-vote requests with a term in our future. If the
        // pre-vote is granted, we will increment our term when we get a
        // quorum. If it is not, the term comes from the node that
        // rejected our vote so we should become a follower at the new
        // term.
      } else {
        LOG_INFO(
            "%llu [term: %llu] received a %s message with higher term from "
            "%llu [term: %llu]",
            id_, term_, raftpb::MessageType_Name(m->type()).c_str(), m->from(),
            m->term());
        if (m->type() == raftpb::MessageType::MsgApp ||
            m->type() == raftpb::MessageType::MsgHeartbeat ||
            m->type() == raftpb::MessageType::MsgSnap) {
          BecomeFollower(m->term(), m->from());
        } else {
          BecomeFollower(m->term(), kNone);
        }
      }
    }
  } else if (m->term() < term_) {
    if ((check_quorum_ || pre_vote_) &&
        (m->type() == raftpb::MessageType::MsgHeartbeat || m->type() == raftpb::MessageType::MsgApp)) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
      auto m = std::make_shared<raftpb::Message>();
      m->set_to(m->from());
      m->set_type(raftpb::MessageType::MsgAppResp);
      Send(m);
    } else if (m->type() == raftpb::MessageType::MsgPreVote) {
      // Before Pre-Vote enable, there may have candidate with higher term,
      // but less log. After update to Pre-Vote, the cluster may deadlock if
      // we drop messages with a lower term.
      LOG_INFO(
          "%llu [logterm: %llu, index: %llu, vote: %llu] rejected %s from %llu "
          "[logterm: %llu, index: %llu] at term %llu",
          id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_,
          raftpb::MessageType_Name(m->type()).c_str(), m->type(), m->from(),
          m->logterm(), m->index(), m->term());
      auto m = std::make_shared<raftpb::Message>();
      m->set_to(m->from());
      m->set_term(term_);
      m->set_type(raftpb::MessageType::MsgPreVoteResp);
      m->set_reject(true);
      Send(m);
    } else {
      // ignore other cases
      LOG_INFO("%llu [term: %llu] ignored a %s message with lower term from %llu [term: %llu]",
        id_, term_, raftpb::MessageType_Name(m->type()).c_str(), m->from(), m->term());
    }
    return Status::OK();
  }

  if (m->type() == raftpb::MessageType::MsgHup) {
    if (pre_vote_) {
      Hup(CampaignType::kPreElection);
    } else {
      Hup(CampaignType::kElection);
    }
  } else if (m->type() == raftpb::MessageType::MsgVote ||
             m->type() == raftpb::MessageType::MsgPreVote) {
    // We can vote if this is a repeat of a vote we've already cast...
    bool can_vote = vote_ == m->from() ||
              			// ...we haven't voted and we don't think there's a leader yet in this term...
                    (vote_ == kNone && lead_ == kNone) ||
                    // ...or this is a PreVote for a future term...
                    (m->type() == raftpb::MessageType::MsgPreVote && m->term() > term_);
    // ...and we believe the candidate is up to date.
    if (can_vote && raft_log_->IsUpToData(m->index(), m->logterm())) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
      LOG_INFO(
          "%llu [logterm: %llu, index: %llu, vote: %llu] cast %s for %llu "
          "[logterm: %llu, index: %llu] at term %llu",
          id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_,
          raftpb::MessageType_Name(m->type()).c_str(), m->from(), m->logterm(),
          m->index(), m->term());
      // When responding to Msg{Pre,}Vote messages we include the term
      // from the message, not the local term. To see why, consider the
      // case where a single node was previously partitioned away and
      // it's local term is now out of date. If we include the local term
      // (recall that for pre-votes we don't update the local term), the
      // (pre-)campaigning node on the other end will proceed to ignore
      // the message (it ignores all out of date messages).
      // The term in the original message and current local term are the
      // same in the case of regular votes, but different for pre-votes.
      auto msg = std::make_shared<raftpb::Message>();
      msg->set_to(m->from());
      msg->set_term(term_);
      msg->set_type(Util::VoteRespMsgType(m->type()));
      Send(msg);
      if (m->type() == raftpb::MessageType::MsgVote) {
        // Only record real votes.
        election_elapsed_ = 0;
        vote_ = m->from();
      }
    } else {
      LOG_INFO(
          "%llu [logterm: %llu, index: %llu, vote: %llu] rejected %s from %llu "
          "[logterm: %llu, index: %llu] at term %llu",
          id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_, raftpb::MessageType_Name(m->type()).c_str(),
          m->logterm(), m->index(), m->term());
      auto msg = std::make_shared<raftpb::Message>();
      msg->set_to(m->from());
      msg->set_term(term_);
      msg->set_type(Util::VoteRespMsgType(m->type()));
      msg->set_reject(true);
      Send(msg);
    }
  } else {
    auto status = Step(m);
    if (!status.IsOK()) {
      return std::move(status);
    }
  }
  return Status::OK();
}

Status StepLeader(MsgPtr m);

Status StepCandidate(MsgPtr m);

Status Raft::StepFollower(MsgPtr m) {
  switch (m->type()) {
    case raftpb::MessageType::MsgProp: {
      if (lead_ == kNone) {
        LOG_INFO("%d no leader at term %d; dropping proposal", id_, term_);
        return Status::Error(kErrProposalDropped);
      } else if (disable_proposal_forwarding_) {
        LOG_INFO("%s not forwarding to leader %d at term %d; dropping proposal",
                 id_, lead_, term_);
        return Status::Error(kErrProposalDropped);
      }
      m->set_to(lead_);
      break;
      ;
    }
    case raftpb::MessageType::MsgApp: {
      election_elapsed_ = 0;
      lead_ = m->from();
      HandleAppendEntries(m);
      break;
    }
    case raftpb::MessageType::MsgHeartbeat: {
      election_elapsed_ = 0;
      lead_ = m->from();
      HandleHearbeat(m);
      break;
    }
    case raftpb::MessageType::MsgSnap: {
      election_elapsed_ = 0;
      lead_ = m->from();
      HandleSnapshot(m);
      break;
    }
    case raftpb::MessageType::MsgTransferLeader: {
      if (lead_ == kNone) {
        LOG_INFO("%d no leader at term %d; dropping leader transfer msg", id_,
                 term_);
        return Status::OK();
      }
      m->set_to(lead_);
      Send(m);
      break;
    }
    case raftpb::MessageType::MsgTimeoutNow: {
      LOG_INFO(
          "%d [term %d] received MsgTimeoutNow from %d and starts an election "
          "to get leadership.",
          id_, term_, m->from());
      // Leadership transfers never use pre-vote even if r.preVote is true; we
      // know we are not recovering from a partition so there is no need for the
      Hup(CampaignType::kTransfer);
      break;
    }
    case raftpb::MessageType::MsgReadIndex: {
      if (lead_ == kNone) {
        LOG_INFO("%d no leader at term %d; dropping index reading msg", id_,
                 term_);
        return Status::OK();
      }
      m->set_to(lead_);
      Send(m);
      break;
    }
    case raftpb::MessageType::MsgReadIndexResp: {
      if (m->entries().size() != 1) {
        LOG_INFO(
            "%d invalid format of MsgReadIndexResp from %d, entries count: %d",
            id_, m->from(), m->entries().size());
      }
      read_states_.emplace_back(ReadState{
          .index_ = m->index(), .request_ctx_ = m->entries()[0].data()});
      break;
    }
  }
  return Status::OK();
}

void HandleAppendEntries(MsgPtr m);

void HandleHearbeat(MsgPtr m);

void HandleSnapshot(MsgPtr m);

void HandleSnapshot(MsgPtr m);

// Restore recovers the state machine from a snapshot. It restores the log and
// the configuration of state machine. If this method returns false, the
// snapshot was ignored, either because it was obsolete or because of an
// error.
bool Restore(SnapshotPtr s);

bool Raft::Promotable() {
  auto pr = trk_.GetProgress(id_);
  return pr != nullptr && !pr->IsLearner() && !raft_log_->HasPendingSnapshot();
}

raftpb::ConfState ApplyConfChange(const raftpb::ConfChangeV2& cc);

raftpb::ConfState Raft::SwitchToConfig(const ProgressTracker::Config& cfg,
                                       const ProgressMap& prs) {
  trk_.SetConfig(cfg);
  trk_.SetProgressMap(prs);

  LOG_INFO("%d switched to configuration %s", id_, cfg.Describe().c_str());

  auto cs = trk_.ConfState();
  auto pr = trk_.GetProgress(id_);

  // Update whether the node itself is a learner, resetting to false when the
  // node is removed.
  if ((!pr || pr->IsLearner()) && state_ == RaftStateType::kLeader) {
    // This node is leader and was removed or demoted. We prevent demotions
    // at the time writing but hypothetically we handle them the same way as
    // removing the leader: stepping down into the next Term.
    //
    // TODO(tbg): step down (for sanity) and ask follower with largest Match
    // to TimeoutNow (to avoid interruption). This might still drop some
    // proposals but it's better than nothing.
    //
    // TODO(tbg): test this branch. It is untested at the time of writing.
    return cs;
  }

  // The remaining steps only make sense if this node is the leader and there
  // are other nodes.
  if (state_ != RaftStateType::kLeader || cs.voters().size() == 0) {
    return cs;
  }

  if (MaybeCommit()) {
    // If the configuration change means that more entries are committed now,
    // broadcast/append to everyone in the updated config.
    BcastAppend();
  } else {
    // Otherwise, still probe the newly added replicas; there's no reason to
    // let them wait out a heartbeat interval (or the next incoming
    // proposal).
    trk_.Visit(
        [this](uint64_t id, ProgressPtr& pr) { MaybeSendAppend(id, false); });
  }
  // If the leadTransferee was removed or demoted, abort the leadership
  // transfer.
  if (trk_.GetConfig().voters_.IDs().count(lead_transferee_) == 0 &&
      lead_transferee_ != 0) {
    AbortLeaderTransfer();
  }
  return cs;
}

void LoadState(const raftpb::HardState& state);

bool Raft::PastElectionTimeout() const {
  return election_elapsed_ >= randomized_election_timeout_;
}

void ResetRandomizedElectionTimeout();

void SendTimeoutNow(uint64_t to);

void AbortLeaderTransfer() { lead_transferee_ = kNone; }

// CommittedEntryinCurrentTerm return true if the peer has committed an entry in
// its term.
bool CommittedEntryinCurrentTerm();

// ResponseToReadIndexReq constructs a response for `req`. If `req` comes from
// the peer itself, a blank value will be returned.
MsgPtr ResponseToReadIndexReq(MsgPtr req, uint64_t read_index);

// IncreaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
bool IncreaseUncommittedSize(const EntryPtrs& ents);

// ReduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
void ReduceUncommittedSize(const EntryPtrs& ents);

int64_t NumOfPendingConf(const EntryPtrs& ents);

void ReleasePendingReadIndexMessages();

void SendMsgReadIndexResponse(MsgPtr m);

};  // namespace craft