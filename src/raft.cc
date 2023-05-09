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

#include <random>

#include "confchange/restore.h"
#include "logger.h"
#include "raftpb/confchange.h"
#include "raftpb/confstate.h"
#include "util.h"

namespace craft {

static const std::string kRaftStateFollower = "RaftStateFollower";
static const std::string kRaftStateCandidate = "RaftStateCandidate";
static const std::string kRaftStateLeader = "RaftStateLeader";
static const std::string kRaftStatePreCandidate = "RaftStatePreCandidate";
static const std::string kRaftStateUnknow = "RaftStateUnknow";

static const std::string kCampaignPreElection = "CampaignPreElection";
static const std::string kCampaignElection = "CampaignElection";
static const std::string kCampaignTransfer = "CampaignTransfer";
static const std::string kCampaignUnknow = "CampaignUnknow";

const std::string& RaftStateTypeName(RaftStateType t) {
  switch (t) {
    case RaftStateType::kFollower:
      return kRaftStateFollower;
    case RaftStateType::kCandidate:
      return kRaftStateCandidate;
    case RaftStateType::kLeader:
      return kRaftStateLeader;
    case RaftStateType::kPreCandidate:
      return kRaftStatePreCandidate;
    default:
      return kRaftStateUnknow;
  }
}

const std::string& CampaignTypeName(CampaignType t) {
  switch (t) {
    case CampaignType::kPreElection:
      return kCampaignPreElection;
    case CampaignType::kElection:
      return kCampaignElection;
    case CampaignType::kTransfer:
      return kCampaignTransfer;
    default:
      return kCampaignUnknow;
  }
}

bool IsEmptyHardState(const raftpb::HardState& st) {
  static raftpb::HardState empty_state;
  return st.term() == empty_state.term() && st.vote() == empty_state.vote() &&
         st.commit() == empty_state.commit();
}

static bool IsEmptySnap(const SnapshotPtr& sp) {
  return sp->metadata().index() == 0;
}

static int64_t NumOfPendingConf(const EntryPtrs& ents) {
  int64_t n = 0;
  for (auto& ent : ents) {
    if (ent->type() == raftpb::EntryType::EntryConfChange ||
        ent->type() == raftpb::EntryType::EntryConfChangeV2) {
      n++;
    }
  }
  return n;
}

uint64_t Ready::AppliedCursor() const {
  auto n = committed_entries.size();
  if (n > 0) {
    return committed_entries[n - 1]->index();
  }
  auto index = snapshot->metadata().index();
  if (index > 0) {
    return index;
  }
  return 0;
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

  auto last_index = raft_log->LastIndex();
  auto r = std::make_unique<Raft>(c, std::move(raft_log));

  auto [cfg, prs, s3] =
      ::craft::Restore(Changer(r->GetTracker(), last_index), cs);
  if (!s3.IsOK()) {
    LOG_FATAL("restore error, err:%s", s3.Str());
  }

  auto cs2 = r->SwitchToConfig(cfg, prs);
  assert(cs == cs2);

  if (!IsEmptyHardState(hs)) {
    r->LoadState(hs);
  }
  if (c.applied > 0) {
    r->GetRaftLog()->AppliedTo(c.applied);
  }

  r->BecomeFollower(r->Term(), kNone);

  std::string nodes_str;
  auto voter_nodes = r->GetTracker().VoterNodes();
  for (size_t i = 0; i < voter_nodes.size(); i++) {
    if (i != 0) {
      nodes_str += ",";
    }
    nodes_str += std::to_string(voter_nodes[i]);
  }
  LOG_INFO(
      "new raft %llu [peers: [%s], term: %llu, commit: %llu, applied: %llu, "
      "lastindex: %llu, lastterm: %llu]",
      r->ID(), nodes_str.c_str(), r->Term(), r->GetRaftLog()->Committed(),
      r->GetRaftLog()->Applied(), r->GetRaftLog()->LastIndex(),
      r->GetRaftLog()->LastTerm());

  return std::move(r);
}

Raft::Raft(const Config& c, std::unique_ptr<RaftLog>&& raft_log)
    : id_(c.id),
      term_(0),
      vote_(kNone),
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

void Raft::Send(MsgPtr m) {
  if (m->from() == kNone) {
    m->set_from(id_);
  }
  if (m->type() == raftpb::MessageType::MsgVote ||
      m->type() == raftpb::MessageType::MsgVoteResp ||
      m->type() == raftpb::MessageType::MsgPreVote ||
      m->type() == raftpb::MessageType::MsgPreVoteResp) {
    if (m->term() == 0) {
      // All {pre-,}campaign messages need to have the term set when
      // sending.
      // - MsgVote: m.Term is the term the node is campaigning for,
      //   non-zero as we increment the term when campaigning.
      // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
      //   granted, non-zero for the same reason MsgVote is
      // - MsgPreVote: m.Term is the term the node will campaign,
      //   non-zero as we use m.Term to indicate the next term we'll be
      //   campaigning for
      // - MsgPreVoteResp: m.Term is the term received in the original
      //   MsgPreVote if the pre-vote was granted, non-zero for the
      //   same reasons MsgPreVote is
      LOG_FATAL("term should be set when sending %s",
                raftpb::MessageType_Name(m->type()).c_str());
    }
  } else {
    if (m->term() != 0) {
      LOG_FATAL("term should not be set when sending %s (was %d)",
                raftpb::MessageType_Name(m->type()).c_str(), m->type());
    }
    // do not attach term to MsgProp, MsgReadIndex
    // proposals are a way to forward to the leader and
    // should be treated as local message.
    // MsgReadIndex is also forwarded to leader.
    if (m->type() != raftpb::MessageType::MsgProp &&
        m->type() != raftpb::MessageType::MsgReadIndex) {
      m->set_term(term_);
    }
  }
  msgs_.emplace_back(m);
}

void Raft::SendAppend(uint64_t to) { MaybeSendAppend(to, true); }

bool Raft::MaybeSendAppend(uint64_t to, bool send_if_empty) {
  auto pr = trk_.GetProgress(to);
  if (pr->IsPaused()) {
    return false;
  }
  auto m = std::make_shared<raftpb::Message>();
  m->set_to(to);

  auto [term, st] = raft_log_->Term(pr->Next() - 1);
  auto [ents, se] = raft_log_->Entries(pr->Next(), max_msg_size_);
  if (ents.empty() && !send_if_empty) {
    return false;
  }

  // send snapshot if we failed to get term or entries
  if (!st.IsOK() || !se.IsOK()) {
    if (!pr->RecentActive()) {
      LOG_DEBUG(
          "ignore sending snapshot to %llu since it is not recently active",
          to);
      return false;
    }

    m->set_type(raftpb::MessageType::MsgSnap);
    auto [snapshot, s] = raft_log_->Snapshot();
    if (!s.IsOK()) {
      if (strstr(s.Str(), kErrSnapshotTemporarilyUnavailable)) {
        LOG_DEBUG(
            "%llu failed to send snapshot to %llu because snapshot is "
            "temporarily unavailable",
            id_, to);
        return false;
      }
      LOG_FATAL("unexpected state!!!");
    }
    if (IsEmptySnap(snapshot)) {
      LOG_FATAL("need non-empty snapshot");
    }
    m->mutable_snapshot()->CopyFrom(*snapshot);
    auto sindex = snapshot->metadata().index();
    auto sterm = snapshot->metadata().term();
    LOG_DEBUG(
        "%llu [firstindex: %llu, commit: %llu] sent snapshot[index: %llu, "
        "term: %llu] to %x [%s]",
        id_, raft_log_->FirstIndex(), raft_log_->Committed(), sindex, sterm, to,
        pr->String().c_str());
    pr->BecomeSnapshot(sindex);
    LOG_DEBUG("%llu paused sending replication messages to %llu [%s]", id_, to,
              pr->String().c_str());
  } else {
    m->set_type(raftpb::MessageType::MsgApp);
    m->set_index(pr->Next() - 1);
    m->set_logterm(term);
    // TODO(JT): zerocopy
    for (auto& ent : ents) {
      m->add_entries()->CopyFrom(*ent);
    }
    m->set_commit(raft_log_->Committed());
    if (!m->entries().empty()) {
      if (pr->State() == StateType::kReplicate) {
        auto last = m->entries().rbegin()->index();
        pr->OptimisticUpdate(last);
        pr->GetInflights()->Add(last);
      } else if (pr->State() == StateType::kProbe) {
        pr->SetProbeSent(true);
      } else {
        // TODO(JT): add StateTypeName
        LOG_FATAL("%llu is sending append in unhandled state %s", id_,
                  pr->State());
      }
    }
  }
  Send(m);
  return true;
}

void Raft::SendHeartbeat(uint64_t to, const std::string& ctx) {
  // Attach the commit as min(to.matched, r.committed).
  // When the leader sends out heartbeat message,
  // the receiver(follower) might not be matched with the leader
  // or it might not have all the committed entries.
  // The leader MUST NOT forward the follower's commit to
  // an unmatched index.
  auto commit = std::min(trk_.GetProgress(to)->Match(), raft_log_->Committed());
  auto m = std::make_shared<raftpb::Message>();
  m->set_to(to);
  m->set_type(raftpb::MessageType::MsgHeartbeat);
  m->set_commit(commit);
  m->set_context(ctx);
  return Send(m);
}

void Raft::BcastAppend() {
  trk_.Visit([this](uint64_t id, ProgressPtr& pr) {
    if (id == id_) {
      return;
    }
    SendAppend(id);
  });
}

void Raft::BcastHeartbeat() {
  auto& last_ctx = read_only_->LastPendingRequestCtx();
  BcastHeartbeatWithCtx(last_ctx);
}

void Raft::BcastHeartbeatWithCtx(const std::string& ctx) {
  trk_.Visit([this, &ctx](uint64_t id, ProgressPtr& pr) {
    if (id == id_) {
      return;
    }
    SendHeartbeat(id, ctx);
  });
}

void Raft::Advance(const Ready& rd) {
  ReduceUncommittedSize(rd.committed_entries);

  // If entries were applied (or a snapshot), update our cursor for
  // the next Ready. Note that if the current HardState contains a
  // new Commit index, this does not mean that we're also applying
  // all of the new entries due to commit pagination by size.
  auto new_applied = rd.AppliedCursor();
  if (new_applied > 0) {
    auto old_applied = raft_log_->Applied();
    raft_log_->AppliedTo(new_applied);

    if (trk_.GetConfig().auto_leave_ && old_applied <= pending_conf_index_ &&
        new_applied >= pending_conf_index_ &&
        state_ == RaftStateType::kLeader) {
			// If the current (and most recent, at least for this leader's term)
			// configuration should be auto-left, initiate that now. We use a
			// nil Data which unmarshals into an empty ConfChangeV2 and has the
			// benefit that appendEntry can never refuse it based on its size
			// (which registers as zero).
      auto ent = std::make_shared<raftpb::Entry>();
      ent->set_type(raftpb::EntryType::EntryConfChangeV2);
			// There's no way in which this proposal should be able to be rejected.
      if (!AppendEntry(ent)) {
        LOG_FATAL("refused un-refusable auto-leaving ConfChangeV2");
      }
      pending_conf_index_ = raft_log_->LastIndex();
      LOG_INFO("initiating automatic transition out of joint configuration");
    }
  }

  if (!rd.entries.empty()) {
    auto& e = *(rd.entries.rbegin());
    raft_log_->StableTo(e->index(), e->term());
  }
  if (!IsEmptySnap(rd.snapshot)) {
    raft_log_->StableSnapTo(rd.snapshot->metadata().index());
  }
}

bool Raft::MaybeCommit() {
  auto mci = trk_.Committed();
  return raft_log_->MaybeCommit(mci, term_);
}

void Raft::Reset(uint64_t term) {
  if (term_ != term) {
    term_ = term;
    vote_ = kNone;
  }
  lead_ = kNone;

  election_elapsed_ = 0;
  heartbeat_elapsed_ = 0;
  ResetRandomizedElectionTimeout();

  AbortLeaderTransfer();

  trk_.ResetVotes();
  trk_.Visit([this](uint64_t id, ProgressPtr& pr) {
    pr =
        std::make_shared<Progress>(GetRaftLog()->LastIndex() + 1, 0,
                                   GetTracker().MaxInflight(), pr->IsLearner(), false);
    if (id == ID()) {
      pr->SetMatch(GetRaftLog()->LastIndex());
    }
  });

  pending_conf_index_ = 0;
  uncommitted_size_ = 0;
  read_only_ = std::make_unique<ReadOnly>(read_only_->Option());
}

bool Raft::AppendEntry(const EntryPtr& e) {
  EntryPtrs ents{e};
  return AppendEntry(ents);
}

bool Raft::AppendEntry(const EntryPtrs& es) {
  auto li = raft_log_->LastIndex();
  for (size_t i = 0; i < es.size(); i++) {
    es[i]->set_term(term_);
    es[i]->set_index(li + static_cast<uint64_t>(1) + static_cast<uint64_t>(i));
  }
  // Track the size of this uncommitted proposal.
  if (!IncreaseUncommittedSize(es)) {
    LOG_DEBUG(
        "%llu appending new entries to log would exceed uncommitted entry size "
        "limit; dropping proposal",
        id_);
    // Drop the proposal.
    return false;
  }
  // use latest "last" index after truncate/append
  li = raft_log_->Append(es);
  trk_.GetProgress(id_)->MaybeUpdate(li);
  // Regardless of maybeCommit's return, our caller will call bcastAppend.
  MaybeCommit();
  return true;
}

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

void Raft::TickHeartbeat() {
  heartbeat_elapsed_++;
  election_elapsed_++;
  if (election_elapsed_ >= election_timeout_) {
    election_elapsed_ = 0;
    if (check_quorum_) {
      auto m = std::make_shared<raftpb::Message>();
      m->set_from(id_);
      m->set_type(raftpb::MessageType::MsgCheckQuorum);
      auto status = Step(m);
      if (!status.IsOK()) {
        LOG_DEBUG("error occurred during checking sending heartbeat: %s",
                  status.Str());
      }
      // If current leader cannot transfer leadership in electionTimeout, it
      // becomes leader again.
      if (state_ == RaftStateType::kLeader && lead_transferee_ != kNone) {
        AbortLeaderTransfer();
      }
    }
  }

  if (state_ != RaftStateType::kLeader) {
    return;
  }

  if (heartbeat_elapsed_ >= heartbeat_timeout_) {
    heartbeat_elapsed_ = 0;
    auto m = std::make_shared<raftpb::Message>();
    m->set_from(id_);
    m->set_type(raftpb::MessageType::MsgBeat);
    auto status = Step(m);
    if (!status.IsOK()) {
      LOG_DEBUG("error occurred during checking sending heartbeat: %s",
                status.Str());
    }
  }
}

void Raft::BecomeFollower(uint64_t term, uint64_t lead) {
  step_ = std::bind(&Raft::StepFollower, this, std::placeholders::_1);
  Reset(term);
  tick_ = std::bind(&Raft::TickElection, this);
  lead_ = lead;
  state_ = RaftStateType::kFollower;
  LOG_INFO("%d became follower at term %d, vote %llu", id_, term_, vote_);
}

void Raft::BecomeCandidate() {
  // TODO: remove the panic when the raft implementation is stable
  if (state_ == RaftStateType::kLeader) {
    LOG_FATAL("invalid transition [leader -> candidate]");
  }
  step_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
  Reset(term_ + 1);
  tick_ = std::bind(&Raft::TickElection, this);
  vote_ = id_;
  state_ = RaftStateType::kCandidate;
  LOG_INFO("%llu became candidate at term %llu", id_, term_);
}

void Raft::BecomePreCandidate() {
  // TODO: remove the panic when the raft implementation is stable
  if (state_ == RaftStateType::kLeader) {
    LOG_FATAL("invalid transition [leader -> pre-candidate]");
  }
  // Becoming a pre-candidate changes our step functions and state,
  // but doesn't change anything else. In particular it does not increase
  // r.Term or change r.Vote.
  step_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
  trk_.ResetVotes();
  tick_ = std::bind(&Raft::TickElection, this);
  lead_ = kNone;
  state_ = RaftStateType::kPreCandidate;
  LOG_INFO("%llu became pre-candidate at term %llu", id_, term_);
}

void Raft::BecomeLeader() {
  // TODO: remove the panic when the raft implementation is stable
  if (state_ == RaftStateType::kFollower) {
    LOG_FATAL("invalid transition [follower -> leader]");
  }
  step_ = std::bind(&Raft::StepLeader, this, std::placeholders::_1);
  Reset(term_);
  tick_ = std::bind(&Raft::TickHeartbeat, this);

  lead_ = id_;
  state_ = RaftStateType::kLeader;
  // Followers enter replicate mode when they've been successfully probed
  // (perhaps after having received a snapshot as a result). The leader is
  // trivially in this state. Note that r.reset() has initialized this
  // progress with the last index already.
  trk_.GetProgress(id_)->BecomeReplicate();

  // Conservatively set the pendingConfIndex to the last index in the
  // log. There may or may not be a pending config change, but it's
  // safe to delay any future proposals until we commit all our
  // pending log entries, and scanning the entire tail of the log
  // could be expensive.
  pending_conf_index_ = raft_log_->LastIndex();

  auto empty_ent = std::make_shared<raftpb::Entry>();
  if (!AppendEntry(empty_ent)) {
    // This won't happen because we just called reset() above.
    LOG_FATAL("empty entry was dropped");
  }
  // As a special case, don't count the initial empty entry towards the
  // uncommitted log quota. This is because we want to preserve the
  // behavior of allowing one entry larger than quota if the current
  // usage is zero.
  ReduceUncommittedSize(EntryPtrs{empty_ent});
  LOG_INFO("%llu became leader at term %llu", id_, term_);
}

void Raft::Hup(CampaignType t) {
  if (state_ == RaftStateType::kLeader) {
    LOG_DEBUG("%llu ignoring MsgHup because already leader", id_);
    return;
  }

  if (!Promotable()) {
    LOG_WARNING("%llu is unpromotable and can not campaign", id_);
    return;
  }
  auto [ents, status] = raft_log_->Slice(raft_log_->Applied() + 1,
                                         raft_log_->Committed() + 1, kNoLimit);
  if (!status.IsOK()) {
    LOG_FATAL("unexpected error getting unapplied entries (%s)", status.Str());
  }
  auto n = NumOfPendingConf(ents);
  if (n != 0 && raft_log_->Committed() > raft_log_->Applied()) {
    LOG_WARNING(
        "%llu cannot campaign at term %llu since there are still %lld pending "
        "configuration changes to apply",
        id_, term_, n);
  }

  LOG_INFO("%llu is starting a new election at term %llu", id_, term_);
  Campaign(t);
}

void Raft::Campaign(CampaignType t) {
  if (!Promotable()) {
    // This path should not be hit (callers are supposed to check), but
    // better safe than sorry.
    LOG_WARNING("%llu is unpromotable; campaign() should have been called",
                id_);
  }
  uint64_t term = 0;
  raftpb::MessageType vote_msg;
  if (t == CampaignType::kPreElection) {
    BecomePreCandidate();
    vote_msg = raftpb::MessageType::MsgPreVote;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = term_ + 1;
  } else {
    BecomeCandidate();
    vote_msg = raftpb::MsgVote;
    term = term_;
  }

  auto [granted, rejected, res] =
      Poll(id_, Util::VoteRespMsgType(vote_msg), true);
  if (res == VoteState::kVoteWon) {
    // We won the election after voting for ourselves (which must mean that
    // this is a single-node cluster). Advance to the next state.
    if (t == CampaignType::kPreElection) {
      Campaign(CampaignType::kElection);
    } else {
      BecomeLeader();
    }
    return;
  }
  auto ids = trk_.VoterNodes();
  for (auto id : ids) {
    if (id == id_) {
      continue;
    }
    LOG_INFO(
        "%llu [logterm: %llu, term: %llu, index: %llu] sent %s request to %llu at term "
        "%llu",
        id_, raft_log_->LastTerm(), term, raft_log_->LastIndex(),
        raftpb::MessageType_Name(vote_msg).c_str(), id, term_);
    auto m = std::make_shared<raftpb::Message>();
    m->set_term(term);
    m->set_to(id);
    m->set_type(vote_msg);
    m->set_index(raft_log_->LastIndex());
    m->set_logterm(raft_log_->LastTerm());
    if (t == CampaignType::kTransfer) {
      m->set_context(CampaignTypeName(CampaignType::kTransfer));
    }
    Send(m);
  }
}

std::tuple<int64_t, int64_t, VoteState> Raft::Poll(uint64_t id,
                                                   raftpb::MessageType t,
                                                   bool v) {
  if (v) {
    LOG_INFO("%llu received %s from %llu at term %llu", id_,
             raftpb::MessageType_Name(t).c_str(), id, term_);
  } else {
    LOG_INFO("%llu received %s rejection from %llu at term %llu", id_,
             raftpb::MessageType_Name(t).c_str(), id, term_);
  }
  trk_.RecordVote(id, v);
  return trk_.TallyVotes();
}

Status Raft::Step(MsgPtr m) {
  // Handle the message term, which may result in our stepping down to a
  // follower.
  if (m->term() == 0) {
    // local message
  } else if (m->term() > term_) {
    if (m->type() == raftpb::MessageType::MsgVote ||
        m->type() == raftpb::MessageType::MsgPreVote) {
      bool force = m->context() == CampaignTypeName(CampaignType::kTransfer);
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
  } else if (m->term() < term_) {
    if ((check_quorum_ || pre_vote_) &&
        (m->type() == raftpb::MessageType::MsgHeartbeat ||
         m->type() == raftpb::MessageType::MsgApp)) {
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
      // disruptive term increases, by notifying leader of this node's
      // activeness. The above comments also true for Pre-Vote
      //
      // When follower gets isolated, it soon starts an election ending
      // up with a higher term than leader, although it won't receive enough
      // votes to win the election. When it regains connectivity, this response
      // with "pb.MsgAppResp" of higher term would force leader to step down.
      // However, this disruption is inevitable to free this stuck node with
      // fresh election. This can be prevented with Pre-Vote phase.
      auto msg = std::make_shared<raftpb::Message>();
      msg->set_to(m->from());
      msg->set_type(raftpb::MessageType::MsgAppResp);
      Send(msg);
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
      auto msg = std::make_shared<raftpb::Message>();
      msg->set_to(m->from());
      msg->set_term(term_);
      msg->set_type(raftpb::MessageType::MsgPreVoteResp);
      msg->set_reject(true);
      Send(msg);
    } else {
      // ignore other cases
      LOG_INFO(
          "%llu [term: %llu] ignored a %s message with lower term from %llu "
          "[term: %llu]",
          id_, term_, raftpb::MessageType_Name(m->type()).c_str(), m->from(),
          m->term());
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
    bool can_vote =
        vote_ == m->from() ||
        // ...we haven't voted and we don't think there's a leader yet in this
        // term...
        (vote_ == kNone && lead_ == kNone) ||
        // ...or this is a PreVote for a future term...
        (m->type() == raftpb::MessageType::MsgPreVote && m->term() > term_);
    // ...and we believe the candidate is up to date.
    if (can_vote && raft_log_->IsUpToData(m->index(), m->logterm())) {
      // Note: it turns out that that learners must be allowed to cast votes.
      // This seems counter- intuitive but is necessary in the situation in
      // which a learner has been promoted (i.e. is now a voter) but has not
      // learned about this yet. For example, consider a group in which id=1 is
      // a learner and id=2 and id=3 are voters. A configuration change
      // promoting 1 can be committed on the quorum `{2,3}` without the config
      // change being appended to the learner's log. If the leader (say 2)
      // fails, there are de facto two voters remaining. Only 3 can win an
      // election (due to its log containing all committed entries), but to do
      // so it will need 1 to vote. But 1 considers itself a learner and will
      // continue to do so until 3 has stepped up as leader, replicates the conf
      // change to 1, and 1 applies it. Ultimately, by receiving a request to
      // vote, the learner realizes that the candidate believes it to be a
      // voter, and that it should act accordingly. The candidate's config may
      // be stale, too; but in that case it won't win the election, at least in
      // the absence of the bug discussed in:
      // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
      LOG_INFO(
          "%llu [logterm: %llu, index: %llu, vote: %llu] cast %s for %llu "
          "[logterm: %llu, term: %llu index: %llu] at term %llu",
          id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_,
          raftpb::MessageType_Name(m->type()).c_str(), m->from(), m->logterm(), m->term(),
          m->index(), term_);
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
      msg->set_term(m->term());
      std::cout << "term: " << m->term() << std::endl;
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
          "[logterm: %llu, term: %llu, index: %llu] at term %llu",
          id_, raft_log_->LastTerm(), raft_log_->LastIndex(), vote_,
          raftpb::MessageType_Name(m->type()).c_str(), m->from(), m->logterm(), m->term(),
          m->index(), term_);
      auto msg = std::make_shared<raftpb::Message>();
      msg->set_to(m->from());
      msg->set_term(term_);
      msg->set_type(Util::VoteRespMsgType(m->type()));
      msg->set_reject(true);
      Send(msg);
    }
  } else {
    auto status = step_(m);
    if (!status.IsOK()) {
      return std::move(status);
    }
  }
  return Status::OK();
}

Status Raft::StepLeader(MsgPtr m) {
  // These message types do not require any progress for m.From.
  switch (m->type()) {
    case raftpb::MessageType::MsgBeat: {
      BcastHeartbeat();
      return Status::OK();
    }
    case raftpb::MessageType::MsgCheckQuorum: {
      // The leader should always see itself as active. As a precaution, handle
      // the case in which the leader isn't in the configuration any more (for
      // example if it just removed itself).
      //
      // TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
      // leader steps down when removing itself. I might be missing something.
      auto pr = trk_.GetProgress(id_);
      if (pr) {
        pr->SetRecentActive(true);
      }
      if (!trk_.QuorumActive()) {
        LOG_WARNING("%llu stepped down to follower since quorum is not active",
                    id_);
        BecomeFollower(term_, kNone);
      }
      // Mark everyone (but ourselves) as inactive in preparation for the next
      // CheckQuorum.
      trk_.Visit([this](uint64_t id, ProgressPtr& pr) {
        if (id != id_) {
          pr->SetRecentActive(false);
        }
      });
      return Status::OK();
    }
    case raftpb::MessageType::MsgProp: {
      if (m->entries().size() == 0) {
        LOG_FATAL("%llu stepped empty MsgProp", id_);
      }
      if (!trk_.GetProgress(id_)) {
        // If we are not currently a member of the range (i.e. this node
        // was removed from the configuration while serving as leader),
        // drop any new proposals.
        return Status::Error(kErrProposalDropped);
      }
      if (lead_transferee_ != kNone) {
        LOG_DEBUG(
            "%llu [term %llu] transfer leadership to %llu is in progress; "
            "dropping proposal",
            id_, term_, lead_transferee_);
        return Status::Error(kErrProposalDropped);
      }

      // TODO(JT): handle , double check
      for (size_t i = 0; i < m->entries_size(); i++) {
        auto& e = m->entries()[i];
        ConfChangeI cc;
        if (e.type() == raftpb::EntryType::EntryConfChange) {
          raftpb::ConfChange ccc;
          if (!ccc.ParseFromString(e.data())) {
            LOG_FATAL("parse confchange error");
          }
          cc.SetConfChange(std::move(ccc));
        } else if (e.type() == raftpb::EntryType::EntryConfChangeV2) {
          raftpb::ConfChangeV2 ccc;
          if (!ccc.ParseFromString(e.data())) {
            LOG_FATAL("parse confchange error");
          }
          cc.SetConfChange(std::move(ccc));
        }
        if (!cc.IsNull()) {
          auto already_pending = pending_conf_index_ > raft_log_->Applied();
          auto already_joint = trk_.GetConfig().voters_.At(1).Size() > 0;
          auto wants_leave_joint = cc.AsV2().changes().size() == 0;

          std::string refused;
          if (already_pending) {
            refused = "possible unapplied conf change";
          } else if (already_joint && !wants_leave_joint) {
            refused = "must transition out of joint config first";
          } else if (!already_joint && wants_leave_joint) {
            refused = "not in joint state; refusing empty conf change";
          }
          if (!refused.empty()) {
            LOG_INFO("%llu ignoring conf change at config %s", id_,
                     refused.c_str());
            raftpb::Entry entry;
            entry.set_type(raftpb::EntryType::EntryNormal);
            (*m->mutable_entries())[i] = entry;
          } else {
            pending_conf_index_ = raft_log_->LastIndex() + i + 1;
          }
        }
      }

      if (!AppendEntry(Util::MakeEntries(m))) {
        return Status::Error(kErrProposalDropped);
      }
      BcastAppend();
      return Status::OK();
    }
    case raftpb::MessageType::MsgReadIndex: {
      // only one voting member (the leader) in the cluster
      if (trk_.IsSingleton()) {
        auto resp = ResponseToReadIndexReq(m, raft_log_->Committed());
        if (resp->to() != kNone) {
          Send(resp);
        }
        return Status::OK();
      }

      // Postpone read only request when this leader has not committed
      // any log entry at its term.
      if (!CommittedEntryinCurrentTerm()) {
        pending_read_index_messages_.emplace_back(m);
        return Status::OK();
      }

      SendMsgReadIndexResponse(m);

      return Status::OK();
    }
  }

  // All other message types require a progress for m.From (pr).
  auto pr = trk_.GetProgress(m->from());
  if (!pr) {
    LOG_DEBUG("%llu no progress available for %llu", id_, m->from());
    return Status::OK();
  }
  switch (m->type()) {
    case raftpb::MessageType::MsgAppResp: {
      pr->SetRecentActive(true);
      if (m->reject()) {
        // RejectHint is the suggested next base entry for appending (i.e.
        // we try to append entry RejectHint+1 next), and LogTerm is the
        // term that the follower has at index RejectHint. Older versions
        // of this library did not populate LogTerm for rejections and it
        // is zero for followers with an empty log.
        //
        // Under normal circumstances, the leader's log is longer than the
        // follower's and the follower's log is a prefix of the leader's
        // (i.e. there is no divergent uncommitted suffix of the log on the
        // follower). In that case, the first probe reveals where the
        // follower's log ends (RejectHint=follower's last index) and the
        // subsequent probe succeeds.
        //
        // However, when networks are partitioned or systems overloaded,
        // large divergent log tails can occur. The naive attempt, probing
        // entry by entry in decreasing order, will be the product of the
        // length of the diverging tails and the network round-trip latency,
        // which can easily result in hours of time spent probing and can
        // even cause outright outages. The probes are thus optimized as
        // described below.
        LOG_DEBUG(
            "%llu received MsgAppResp(rejected, hint: (index %llu, term %llu)) "
            "from %llu for index %llu",
            id_, m->rejecthint(), m->logterm(), m->from(), m->index());
        auto next_probe_idx = m->rejecthint();
        if (m->logterm() > 0) {
          // If the follower has an uncommitted log tail, we would end up
          // probing one by one until we hit the common prefix.
          //
          // For example, if the leader has:
          //
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 3 3 3 5 5 5 5 5
          //   term (F)   1 1 1 1 2 2
          //
          // Then, after sending an append anchored at (idx=9,term=5) we
          // would receive a RejectHint of 6 and LogTerm of 2. Without the
          // code below, we would try an append at index 6, which would
          // fail again.
          //
          // However, looking only at what the leader knows about its own
          // log and the rejection hint, it is clear that a probe at index
          // 6, 5, 4, 3, and 2 must fail as well:
          //
          // For all of these indexes, the leader's log term is larger than
          // the rejection's log term. If a probe at one of these indexes
          // succeeded, its log term at that index would match the leader's,
          // i.e. 3 or 5 in this example. But the follower already told the
          // leader that it is still at term 2 at index 6, and since the
          // log term only ever goes up (within a log), this is a contradiction.
          //
          // At index 1, however, the leader can draw no such conclusion,
          // as its term 1 is not larger than the term 2 from the
          // follower's rejection. We thus probe at 1, which will succeed
          // in this example. In general, with this approach we probe at
          // most once per term found in the leader's log.
          //
          // There is a similar mechanism on the follower (implemented in
          // handleAppendEntries via a call to findConflictByTerm) that is
          // useful if the follower has a large divergent uncommitted log
          // tail[1], as in this example:
          //
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 3 3 3 3 3 3 3 7
          //   term (F)   1 3 3 4 4 5 5 5 6
          //
          // Naively, the leader would probe at idx=9, receive a rejection
          // revealing the log term of 6 at the follower. Since the leader's
          // term at the previous index is already smaller than 6, the leader-
          // side optimization discussed above is ineffective. The leader thus
          // probes at index 8 and, naively, receives a rejection for the same
          // index and log term 5. Again, the leader optimization does not
          // improve over linear probing as term 5 is above the leader's term 3
          // for that and many preceding indexes; the leader would have to probe
          // linearly until it would finally hit index 3, where the probe would
          // succeed.
          //
          // Instead, we apply a similar optimization on the follower. When the
          // follower receives the probe at index 8 (log term 3), it concludes
          // that all of the leader's log preceding that index has log terms of
          // 3 or below. The largest index in the follower's log with a log term
          // of 3 or below is index 3. The follower will thus return a rejection
          // for index=3, log term=3 instead. The leader's next probe will then
          // succeed at that index.
          //
          // [1]: more precisely, if the log terms in the large uncommitted
          // tail on the follower are larger than the leader's. At first,
          // it may seem unintuitive that a follower could even have such
          // a large tail, but it can happen:
          //
          // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 2 2     [crashes]
          //   term (F)   1
          //   term (F)   1
          //
          // 2. a follower becomes leader and appends entries at term 3.
          //              -----------------
          //   term (x)   1 2 2     [down]
          //   term (F)   1 3 3 3 3
          //   term (F)   1
          //
          // 3. term 3 leader goes down, term 2 leader returns as term 4
          //    leader. It commits the log & entries at term 4.
          //
          //              -----------------
          //   term (L)   1 2 2 2
          //   term (x)   1 3 3 3 3 [down]
          //   term (F)   1
          //              -----------------
          //   term (L)   1 2 2 2 4 4 4
          //   term (F)   1 3 3 3 3 [gets probed]
          //   term (F)   1 2 2 2 4 4 4
          //
          // 4. the leader will now probe the returning follower at index
          //    7, the rejection points it at the end of the follower's log
          //    which is at a higher log term than the actually committed
          //    log.
          next_probe_idx =
              raft_log_->FindConflictByTerm(m->rejecthint(), m->logterm());
        }
        if (pr->MaybeDecrTo(m->index(), next_probe_idx)) {
          LOG_DEBUG("%llu decreased progress of %llu to [%s]", id_, m->from(),
                    pr->String().c_str());
          if (pr->State() == StateType::kReplicate) {
            pr->BecomeProbe();
          }
          SendAppend(m->from());
        }
      } else {
        auto old_paused = pr->IsPaused();
        if (pr->MaybeUpdate(m->index())) {
          if (pr->State() == StateType::kProbe) {
            pr->BecomeReplicate();
          } else if (pr->State() == StateType::kSnapshot &&
                     pr->Match() >= pr->PendingSnapshot()) {
            // LOG_DEBUG();
            pr->BecomeProbe();
            pr->BecomeReplicate();
          } else if (pr->State() == StateType::kReplicate) {
            pr->GetInflights()->FreeLE(m->index());
          }

          if (MaybeCommit()) {
            // committed index has progressed for the term, so it is safe
            // to respond to pending read index requests
            ReleasePendingReadIndexMessages();
            BcastAppend();
          } else if (old_paused) {
            // If we were paused before, this node may be missing the
            // latest commit index, so send it.
            SendAppend(m->from());
          }
          // We've updated flow control information above, which may
          // allow us to send multiple (size-limited) in-flight messages
          // at once (such as when transitioning from probe to
          // replicate, or when freeTo() covers multiple messages). If
          // we have more entries to send, send as many messages as we
          // can (without sending empty messages for the commit index)
          while (MaybeSendAppend(m->from(), false)) {
          }
          // Transfer leadership is in progress.
          if (m->from() == lead_transferee_ &&
              pr->Match() == raft_log_->LastIndex()) {
            LOG_INFO(
                "%llu sent MsgTimeoutNow to %llu after received MsgAppResp",
                id_, m->from());
            SendTimeoutNow(m->from());
          }
        }
      }
      break;
    }
    case raftpb::MessageType::MsgHeartbeatResp: {
      pr->SetRecentActive(true);
      pr->SetProbeSent(false);

      // free one slot for the full inflights window to allow progress.
      if (pr->State() == StateType::kReplicate &&
          pr->GetInflights()->Full()) {
        pr->GetInflights()->FreeFirstOne();
      }
      if (pr->Match() < raft_log_->LastIndex()) {
        SendAppend(m->from());
      }

      if (read_only_->Option() != ReadOnly::ReadOnlyOption::kSafe ||
          m->context().empty()) {
        return Status::OK();
      }

      if (trk_.GetConfig().voters_.VoteResult(read_only_->RecvAck(
              m->from(), m->context())) != VoteState::kVoteWon) {
        return Status::OK();
      }
      auto rss = read_only_->Advance(m);
      for (auto& rs : rss) {
        auto resp = ResponseToReadIndexReq(rs->req, rs->index);
        if (resp->to() != kNone) {
          Send(resp);
        }
      }
      break;
    }
    case raftpb::MessageType::MsgSnapStatus: {
      if (pr->State() != StateType::kSnapshot) {
        return Status::OK();
      }
      // TODO(tbg): this code is very similar to the snapshot handling in
      // MsgAppResp above. In fact, the code there is more correct than the
      // code here and should likely be updated to match (or even better, the
      // logic pulled into a newly created Progress state machine handler).
      if (!m->reject()) {
        pr->BecomeProbe();
        LOG_DEBUG(
            "%llu snapshot succeeded, resumed sending replication messages to "
            "%llu [%s]",
            id_, m->from(), pr->String().c_str());
      } else {
        // NB: the order here matters or we'll be probing erroneously from
        // the snapshot index, but the snapshot never applied.
        pr->SetPendingSnapshot(0);
        pr->BecomeProbe();
        LOG_DEBUG(
            "%llu snapshot failed, resumed sending replication messages to "
            "%llu [%s]",
            id_, m->from(), pr->String().c_str());
      }
      // If snapshot finish, wait for the MsgAppResp from the remote node before
      // sending out the next MsgApp. If snapshot failure, wait for a heartbeat
      // interval before next try
      pr->SetProbeSent(true);
      break;
    }
    case raftpb::MessageType::MsgUnreachable: {
      // During optimistic replication, if the remote becomes unreachable,
      // there is huge probability that a MsgApp is lost.
      if (pr->State() == StateType::kReplicate) {
        pr->BecomeProbe();
      }
      LOG_DEBUG(
          "%x failed to send message to %x because it is unreachable [%s]", id_,
          m->from(), pr->String().c_str());
      break;
    }
    case raftpb::MessageType::MsgTransferLeader: {
      if (pr->IsLearner()) {
        LOG_DEBUG("%llu is learner. Ignored transferring leadership", id_);
        return Status::OK();
      }
      auto lead_transferee = m->from();
      auto last_lead_transferee = lead_transferee_;
      if (last_lead_transferee != kNone) {
        if (last_lead_transferee == lead_transferee) {
          LOG_INFO(
              "%llu [term %llu] transfer leadership to %llu is in progress, "
              "ignores request to same node %llu",
              id_, term_, lead_transferee, lead_transferee);
          return Status::OK();
        }
      }
      if (lead_transferee == id_) {
        LOG_DEBUG(
            "%llu is already leader. Ignored transferring leadership to self",
            id_);
        return Status::OK();
      }
      // Transfer leadership to third party.
      LOG_INFO("%llu [term %llu] starts to transfer leadership to %llu", id_,
               term_, lead_transferee);
      // Transfer leadership should be finished in one electionTimeout, so reset
      // r.electionElapsed.
      election_elapsed_ = 0;
      lead_transferee_ = lead_transferee;
      if (pr->Match() == raft_log_->LastIndex()) {
        SendTimeoutNow(lead_transferee);
        LOG_INFO(
            "%llu sends MsgTimeoutNow to %llu immediately as %llu already has "
            "up-to-date log",
            id_, lead_transferee, lead_transferee);
      } else {
        SendAppend(lead_transferee);
      }
      break;
    }
  }
  return Status::OK();
}

Status Raft::StepCandidate(MsgPtr m) {
  // Only handle vote responses corresponding to our candidacy (while in
  // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
  // our pre-candidate state).
  switch (m->type()) {
    case raftpb::MessageType::MsgProp: {
      LOG_INFO("%llu no leader at term %llu; dropping proposal", id_, term_);
      return Status::Error(kErrProposalDropped);
    }
    case raftpb::MessageType::MsgApp: {
      BecomeFollower(m->term(), m->from());  // always m.term == r.term
      HandleAppendEntries(m);
      return Status::OK();
    }
    case raftpb::MessageType::MsgHeartbeat: {
      BecomeFollower(m->term(), m->from());  // always m.term == r.term
      HandleHearbeat(m);
      return Status::OK();
    }
    case raftpb::MessageType::MsgSnap: {
      BecomeFollower(m->term(), m->from());  // always m.term == r.term
      HandleSnapshot(m);
      return Status::OK();
    }
    case raftpb::MessageType::MsgPreVoteResp:
    case raftpb::MessageType::MsgVoteResp: {
      auto [gr, rj, res] = Poll(m->from(), m->type(), !m->reject());
      LOG_INFO("%llu has received %lld %s votes and %lld vote rejections", id_,
               gr, raftpb::MessageType_Name(m->type()).c_str(), rj);
      if (res == VoteState::kVoteWon) {
        if (state_ == RaftStateType::kPreCandidate) {
          Campaign(CampaignType::kElection);
        } else {
          BecomeLeader();
          BcastAppend();
        }
      } else if (res == VoteState::kVoteLost) {
        // pb.MsgPreVoteResp contains future term of pre-candidate
        // m.Term > r.Term; reuse r.Term
        BecomeFollower(term_, kNone);
      } else {
        // do nothing
      }
      return Status::OK();
    }
    case raftpb::MessageType::MsgTimeoutNow: {
      LOG_DEBUG("%llu [term %llu state %s] ignored MsgTimeoutNow from %llu",
                id_, term_, RaftStateTypeName(state_).c_str(), m->from());
      return Status::OK();
    }
  }
  return Status::OK();
}

Status Raft::StepFollower(MsgPtr m) {
  switch (m->type()) {
    case raftpb::MessageType::MsgProp: {
      if (lead_ == kNone) {
        LOG_INFO("%llu no leader at term %llu; dropping proposal", id_, term_);
        return Status::Error(kErrProposalDropped);
      } else if (disable_proposal_forwarding_) {
        LOG_INFO("%llu not forwarding to leader %llu at term %llu; dropping proposal",
                 id_, lead_, term_);
        return Status::Error(kErrProposalDropped);
      }
      m->set_to(lead_);
      Send(m);
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
          .index = m->index(), .request_ctx = m->entries()[0].data()});
      break;
    }
  }
  return Status::OK();
}

void Raft::HandleAppendEntries(MsgPtr m) {
  if (m->index() < raft_log_->Committed()) {
    auto resp = std::make_shared<raftpb::Message>();
    resp->set_to(m->from());
    resp->set_type(raftpb::MessageType::MsgAppResp);
    resp->set_index(raft_log_->Committed());
    Send(resp);
    return;
  }

  auto [last_index, ok] = raft_log_->MaybeAppend(
      m->index(), m->logterm(), m->commit(), Util::MakeEntries(m));
  if (ok) {
    auto resp = std::make_shared<raftpb::Message>();
    resp->set_to(m->from());
    resp->set_type(raftpb::MessageType::MsgAppResp);
    resp->set_index(last_index);
    Send(resp);
  } else {
    LOG_DEBUG(
        "%llu [logterm: %llu, index: %llu] rejected MsgApp [logterm: %llu, "
        "index: %llu] from %llu",
        id_, raft_log_->ZeroTermOnErrCompacted(raft_log_->Term(m->index())),
        m->index(), m->logterm(), m->index(), m->from());
    // Return a hint to the leader about the maximum index and term that the
    // two logs could be divergent at. Do this by searching through the
    // follower's log for the maximum (index, term) pair with a term <= the
    // MsgApp's LogTerm and an index <= the MsgApp's Index. This can help
    // skip all indexes in the follower's uncommitted tail with terms
    // greater than the MsgApp's LogTerm.
    //
    // See the other caller for findConflictByTerm (in stepLeader) for a much
    // more detailed explanation of this mechanism.
    auto hint_index = std::min(m->index(), raft_log_->LastIndex());
    hint_index = raft_log_->FindConflictByTerm(hint_index, m->logterm());
    auto [hint_term, status] = raft_log_->Term(hint_index);
    if (!status.IsOK()) {
      LOG_FATAL("term(%llu) must be valid, but got %s", hint_index,
                status.Str());
    }
    auto resp = std::make_shared<raftpb::Message>();
    resp->set_to(m->from());
    resp->set_type(raftpb::MessageType::MsgAppResp);
    resp->set_index(m->index());
    resp->set_reject(true);
    resp->set_rejecthint(hint_index);
    resp->set_logterm(hint_term);
    Send(resp);
  }
}

void Raft::HandleHearbeat(MsgPtr m) {
  raft_log_->CommitTo(m->commit());
  auto resp = std::make_shared<raftpb::Message>();
  resp->set_to(m->from());
  resp->set_type(raftpb::MessageType::MsgHeartbeatResp);
  resp->set_context(m->context());
  Send(resp);
}

void Raft::HandleSnapshot(MsgPtr m) {
  auto sindex = m->snapshot().metadata().index();
  auto sterm = m->snapshot().metadata().term();
  if (Restore(m->mutable_snapshot())) {
    LOG_INFO("%llu [commit: %llu] restored snapshot [index: %llu, term: %llu]",
             id_, raft_log_->Committed(), sindex, sterm);
    auto resp = std::make_shared<raftpb::Message>();
    resp->set_to(m->from());
    resp->set_type(raftpb::MessageType::MsgAppResp);
    resp->set_index(raft_log_->LastIndex());
    Send(resp);
  } else {
    LOG_INFO("%llu [commit: %llu] ignored snapshot [index: %llu, term: %llu]",
             id_, raft_log_->Committed(), sindex, sterm);
    auto resp = std::make_shared<raftpb::Message>();
    resp->set_to(m->from());
    resp->set_type(raftpb::MessageType::MsgAppResp);
    resp->set_index(raft_log_->Committed());
    Send(resp);
  }
}

bool Raft::Restore(raftpb::Snapshot* s) {
  if (s->metadata().index() <= raft_log_->Committed()) {
    return false;
  }
  if (state_ != RaftStateType::kFollower) {
    // This is defense-in-depth: if the leader somehow ended up applying a
    // snapshot, it could move into a new term without moving into a
    // follower state. This should never fire, but if it did, we'd have
    // prevented damage by returning early, so log only a loud warning.
    //
    // At the time of writing, the instance is guaranteed to be in follower
    // state when this method is called.
    LOG_WARNING(
        "%llu attempted to restore snapshot as leader; should never happen",
        id_);
    BecomeFollower(term_ + 1, kNone);
    return false;
  }

  // More defense-in-depth: throw away snapshot if recipient is not in the
  // config. This shouldn't ever happen (at the time of writing) but lots of
  // code here and there assumes that r.id is in the progress tracker.
  bool found = false;
  auto cs = s->metadata().conf_state();

  // `LearnersNext` doesn't need to be checked. According to the rules, if a
  // peer in `LearnersNext`, it has to be in `VotersOutgoing`.
  for (auto id : cs.voters()) {
    if (id == id_) {
      found = true;
      break;
    }
  }
  if (!found) {
    for (auto id : cs.learners()) {
      if (id == id_) {
        found = true;
        break;
      }
    }
  }
  if (!found) {
    for (auto id : cs.voters_outgoing()) {
      if (id == id_) {
        found = true;
        break;
      }
    }
  }
  if (!found) {
    LOG_WARNING(
        "%llu attempted to restore snapshot but it is not in the ConfState; "
        "should never happen",
        id_);
    return false;
  }

  // Now go ahead and actually restore.
  if (raft_log_->MatchTerm(s->metadata().index(), s->metadata().term())) {
    LOG_INFO(
        "%x [commit: %llu, lastindex: %llu, lastterm: %llu] fast-forwarded "
        "commit "
        "to snapshot [index: %llu, term: %llu]",
        id_, raft_log_->Committed(), raft_log_->LastIndex(),
        raft_log_->LastTerm(), s->metadata().index(), s->metadata().term());
    raft_log_->CommitTo(s->metadata().index());
    return false;
  }

  // TODO(JT): 确保s的生命周期
  raft_log_->Restore(std::shared_ptr<raftpb::Snapshot>(s));

  // Reset the configuration and add the (potentially updated) peers in anew.
  trk_ = ProgressTracker(trk_.MaxInflight());
  auto [cfg, prs, status] =
      ::craft::Restore(Changer(trk_, raft_log_->LastIndex()), cs);
  if (!status.IsOK()) {
    // This should never happen. Either there's a bug in our config change
    // handling or the client corrupted the conf change.
    LOG_FATAL("unable to restore config %s", status.Str());
    return false;
  }

  auto cs2 = SwitchToConfig(cfg, prs);
  assert(cs == cs2);

  auto pr = trk_.GetProgress(id_);
  pr->MaybeUpdate(pr->Next() -
                  1);  // TODO(tbg): this is untested and likely unneeded

  LOG_INFO(
      "%llu [commit: %llu, lastindex: %llu, lastterm: %llu] restored snapshot "
      "[index: %llu, term: %llu]",
      id_, raft_log_->Committed(), raft_log_->LastIndex(),
      raft_log_->LastTerm(), s->metadata().index(), s->metadata().term());
  return true;
}

bool Raft::Promotable() {
  auto pr = trk_.GetProgress(id_);
  return pr != nullptr && !pr->IsLearner() && !raft_log_->HasPendingSnapshot();
}

raftpb::ConfState Raft::ApplyConfChange(raftpb::ConfChangeV2&& cc) {
  auto [cfg, prs, status] = [this, &cc]() {
    auto changer = Changer(trk_, raft_log_->LastIndex());
    if (LeaveJoint(cc)) {
      return changer.LeaveJoint();
    }

    std::vector<raftpb::ConfChangeSingle> ccs;
    for (size_t i = 0; i < cc.changes_size(); i++) {
      ccs.emplace_back(cc.changes(i));
    }

    auto [auto_leave, ok] = EnterJoint(cc);
    if (ok) {
      return changer.EnterJoint(auto_leave, ccs);
    }
    return changer.Simple(ccs);
  }();
  if (!status.IsOK()) {
    LOG_FATAL("unexpected %s", status.Str());
  }
  return SwitchToConfig(cfg, prs);
}

raftpb::ConfState Raft::SwitchToConfig(const ProgressTracker::Config& cfg,
                                       const ProgressMap& prs) {
  trk_.SetConfig(cfg);
  trk_.SetProgressMap(prs);

  LOG_INFO("%d switched to configuration %s", id_, cfg.String().c_str());

  auto cs = trk_.ConfState();
  auto pr = trk_.GetProgress(id_);

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
  is_learner_ = pr && pr->IsLearner();

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

void Raft::LoadState(const raftpb::HardState& state) {
  if (state.commit() < raft_log_->Committed() ||
      state.commit() > raft_log_->LastIndex()) {
    LOG_FATAL("%llu state.commit %llu is out of range [%llu, %llu]", id_,
              state.commit(), raft_log_->Committed(), raft_log_->LastIndex());
  }
  raft_log_->SetCommitted(state.commit());
  term_ = state.term();
  vote_ = state.vote();
}

bool Raft::PastElectionTimeout() const {
  return election_elapsed_ >= randomized_election_timeout_;
}

// TODO(juntaosu): 实现Util::random函数
void Raft::ResetRandomizedElectionTimeout() {
  std::random_device seed;
  std::ranlux48 engine(seed());
  std::uniform_int_distribution<int64_t> distrib(static_cast<int64_t>(0),
                                                 election_timeout_-1);
  auto random = distrib(engine);
  randomized_election_timeout_ = election_timeout_ + random;
}

void Raft::SendTimeoutNow(uint64_t to) {
  auto m = std::make_shared<raftpb::Message>();
  m->set_to(to);
  m->set_type(raftpb::MessageType::MsgTimeoutNow);
  Send(m);
}

bool Raft::CommittedEntryinCurrentTerm() {
  return raft_log_->ZeroTermOnErrCompacted(
             raft_log_->Term(raft_log_->Committed())) == term_;
}

MsgPtr Raft::ResponseToReadIndexReq(MsgPtr req, uint64_t read_index) {
  if (req->from() == kNone || req->from() == id_) {
    read_states_.emplace_back(
        ReadState{.index = read_index, .request_ctx = req->entries(0).data()});
    return std::make_shared<raftpb::Message>();
  }
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgReadIndexResp);
  m->set_to(req->from());
  m->set_index(read_index);
  // TODO(JT): move?
  m->mutable_entries()->CopyFrom(req->entries());
  return m;
}

bool Raft::IncreaseUncommittedSize(const EntryPtrs& ents) {
  uint64_t s = 0;
  for (auto& e : ents) {
    s += static_cast<uint64_t>(Util::PayloadSize(e));
  }

  if (uncommitted_size_ > 0 && s > 0 &&
      (uncommitted_size_ + s > max_uncommitted_size_)) {
    // If the uncommitted tail of the Raft log is empty, allow any size
    // proposal. Otherwise, limit the size of the uncommitted tail of the
    // log and drop any proposal that would push the size over the limit.
    // Note the added requirement s>0 which is used to make sure that
    // appending single empty entries to the log always succeeds, used both
    // for replicating a new leader's initial empty entry, and for
    // auto-leaving joint configurations.
    return false;
  }
  uncommitted_size_ += s;
  return true;
}

void Raft::ReduceUncommittedSize(const EntryPtrs& ents) {
  if (uncommitted_size_ == 0) {
    // Fast-path for followers, who do not track or enforce the limit.
    return;
  }

  uint64_t s = 0;
  for (auto& e : ents) {
    s += static_cast<uint64_t>(Util::PayloadSize(e));
  }
  if (s > uncommitted_size_) {
    uncommitted_size_ = 0;
  } else {
    uncommitted_size_ -= s;
  }
}

int64_t Raft::NumOfPendingConf(const EntryPtrs& ents) {
  int64_t n = 0;
  for (auto& ent : ents) {
    if (ent->type() == raftpb::EntryType::EntryConfChange ||
        ent->type() == raftpb::EntryType::EntryConfChangeV2) {
      n++;
    }
  }
  return n;
}

void Raft::ReleasePendingReadIndexMessages() {
  if (!CommittedEntryinCurrentTerm()) {
    LOG_ERROR(
        "pending MsgReadIndex should be released only after first commit in "
        "current term");
    return;
  }

  auto msgs = std::move(pending_read_index_messages_);
  for (auto& m : msgs) {
    SendMsgReadIndexResponse(m);
  }
}

void Raft::SendMsgReadIndexResponse(MsgPtr m) {
  // thinking: use an internally defined context instead of the user given
  // context. We can express this in terms of the term and index instead of a
  // user-supplied value. This would allow multiple reads to piggyback on the
  // same message.
  // If more than the local vote is needed, go through a full broadcast.
  if (read_only_->Option() == ReadOnly::ReadOnlyOption::kSafe) {
    read_only_->AddRequest(raft_log_->Committed(), m);
    read_only_->RecvAck(id_, m->entries(0).data());
    BcastHeartbeatWithCtx(m->entries(0).data());
  } else if (read_only_->Option() == ReadOnly::ReadOnlyOption::kLeaseBased) {
    auto resp = ResponseToReadIndexReq(m, raft_log_->Committed());
    if (resp->to() != kNone) {
      Send(resp);
    }
  }
}

}  // namespace craft