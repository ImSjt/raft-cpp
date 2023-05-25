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
#include "src/rawnode.h"
#include "src/util.h"

namespace craft {

static std::tuple<MsgPtr, Status> ConfChangeToMsg(const ConfChangeI& cc) {
  auto [type, data, ok] = cc.Marshal();
  if (!ok) {
    return std::make_tuple(MsgPtr(), Status::Error("marshal error"));
  }
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgProp);
  auto e = m->add_entries();
  e->set_type(type);
  e->set_data(data);
  return std::make_tuple(m, Status::OK());
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
static bool MustSync(const raftpb::HardState& st, const raftpb::HardState& prevst, size_t entsum) {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
  return entsum != 0 || st.vote() != prevst.vote() || st.term() != prevst.term();
}

bool operator==(const raftpb::HardState& s1, const raftpb::HardState& s2) {
  return s1.term() == s2.term() && s1.vote() == s2.vote() && s1.commit() == s2.commit();
}

std::unique_ptr<RawNode> RawNode::New(Raft::Config& c) {
  auto raft = Raft::New(c);
  auto rn = std::make_unique<RawNode>(std::move(raft));
  return rn;
}

Status RawNode::Bootstrap(std::vector<Peer> peers) {
  if (peers.empty()) {
    return Status::Error("must provide at least one peer to Bootstrap");
  }
  auto [last_index, s] = raft_->GetRaftLog()->GetStorage()->LastIndex();
  if (!s.IsOK()) {
    return s;
  }

	// We've faked out initial entries above, but nothing has been
	// persisted. Start with an empty HardState (thus the first Ready will
	// emit a HardState update for the app to persist).
  prev_hard_st_.Clear();

	// TODO(tbg): remove StartNode and give the application the right tools to
	// bootstrap the initial membership in a cleaner way.
  raft_->BecomeFollower(1, Raft::kNone);

  craft::EntryPtrs ents;
  for (size_t i = 0; i < peers.size(); i++) {
    const auto& peer = peers[i];
    raftpb::ConfChange cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(peer.id);
    cc.set_context(peer.context);
    auto data = cc.SerializeAsString();
    auto ent = std::make_shared<raftpb::Entry>();
    ent->set_type(raftpb::EntryType::EntryConfChange);
    ent->set_term(1);
    ent->set_index(static_cast<uint64_t>(i+1));
    ent->set_data(std::move(data));
    ents.emplace_back(ent);
  }
  raft_->GetRaftLog()->Append(ents);

	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	//
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
  raft_->GetRaftLog()->SetCommitted(static_cast<uint64_t>(ents.size()));
  for (const auto& peer : peers) {
    raftpb::ConfChange cc;
    cc.set_node_id(peer.id);
    cc.set_type(raftpb::ConfChangeAddNode);
    raft_->ApplyConfChange(ConfChangeI(std::move(cc)).AsV2());
  }
  return Status::OK();
}

void RawNode::Tick() {
  raft_->Tick();
}

void RawNode::TickQuiesced() {
  raft_->TickQuiesced();
}

Status RawNode::Campaign() {
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgHup);
  return raft_->Step(m);
}

Status RawNode::Propose(const std::string& data) {
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgProp);
  m->set_from(raft_->ID());
  auto e = m->add_entries();
  e->set_data(data);
  return raft_->Step(m);
}

Status RawNode::ProposeConfChange(const ConfChangeI& cc) {
  auto [m, s] = ConfChangeToMsg(cc);
  if (!s.IsOK()) {
    return s;
  }
  return raft_->Step(m);
}

raftpb::ConfState RawNode::ApplyConfChange(const ConfChangeI& cc) {
  return raft_->ApplyConfChange(cc.AsV2());
}

Status RawNode::Step(MsgPtr m) {
	// ignore unexpected local messages receiving over network
  if (Util::IsLocalMsg(m->type())) {
    return Status::Error(kErrStepLocalMsg);
  }
  auto pr = raft_->GetTracker().GetProgress(m->from());
  if (pr || !Util::IsResponseMsg(m->type())) {
    return raft_->Step(m);
  }
  return Status::Error(kErrStepPeerNotFound);
}

std::shared_ptr<Ready> RawNode::GetReady() {
  ready_ = ReadyWithoutAccept();
  AcceptReady(ready_);
  return ready_;
}

std::shared_ptr<Ready> RawNode::ReadyWithoutAccept() {
  return NewReady(raft_.get(), prev_soft_st_, prev_hard_st_);
}

void RawNode::AcceptReady(std::shared_ptr<Ready> rd) {
  if (rd->soft_state.has_value()) {
    prev_soft_st_ = *(rd->soft_state);
  }
  if (!rd->read_states.empty()) {
    raft_->ClearReadStates();
  }
  raft_->ClearMsgs();
}

bool RawNode::HasReady() const {
  if (!(raft_->GetSoftState() == prev_soft_st_)) {
    return true;
  }
  auto hard_st = raft_->GetHardState();
  if (!IsEmptyHardState(hard_st) && !(hard_st == prev_hard_st_)) {
    return true;
  }
  if (raft_->GetRaftLog()->HasPendingSnapshot()) {
    return true;
  }
  if (!raft_->Msgs().empty() ||
      !raft_->GetRaftLog()->UnstableEntries().empty() ||
      raft_->GetRaftLog()->HasNextEnts()) {
    return true;
  }
  if (!raft_->GetReadStates().empty()) {
    return true;
  }
  return false;
}

void RawNode::Advance() {
  if (!IsEmptyHardState(ready_->hard_state)) {
    prev_hard_st_ = ready_->hard_state;
  }
  raft_->Advance(*ready_);
  ready_.reset();
}

NodeStatus RawNode::GetStatus() const {
  NodeStatus s;
  s.basic = GetBasicStatus();
  if (s.basic.soft_state.raft_state == RaftStateType::kLeader) {
    for (auto& p : raft_->GetTracker().GetProgressMap()) {
      s.progress[p.first] = p.second->Clone();
    }
  }
  s.config = raft_->GetTracker().GetConfig();
  return s;
}

NodeBasicStatus RawNode::GetBasicStatus() const {
  NodeBasicStatus s;
  s.id = raft_->ID();
  s.lead_transferee = raft_->LeadTransferee();
  s.hard_state = raft_->GetHardState();
  s.soft_state = raft_->GetSoftState();
  s.applied = raft_->GetRaftLog()->Applied();
  return s;
}

void RawNode::WithProgress(Visitor&& visitor) {
  raft_->GetTracker().Visit([visitor = std::move(visitor)](uint64_t id, ProgressPtr& pr) {
    ProgressType type = ProgressType::kPeer;
    if (pr->IsLearner()) {
      type = ProgressType::kLearner;
    }
    auto p = pr->Clone();
    visitor(id, type, p);
  });
}

void RawNode::ReportUnreachable(uint64_t id) {
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgUnreachable);
  m->set_from(id);
  raft_->Step(m);
}

void RawNode::ReportSnapshot(uint64_t id, SnapshotStatus status) {
  bool rej = status == SnapshotStatus::kFailure;
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgSnapStatus);
  m->set_from(id);
  m->set_reject(rej);
  raft_->Step(m);
}

void RawNode::TransferLeader(uint64_t transferee) {
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgTransferLeader);
  m->set_from(transferee);
  raft_->Step(m);
}

void RawNode::ReadIndex(const std::string& rctx) {
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgReadIndex);
  auto e = m->add_entries();
  e->set_data(rctx);
  raft_->Step(m);
}

std::shared_ptr<Ready> NewReady(Raft* raft, const SoftState& prev_soft_st, const raftpb::HardState& prev_hard_st) {
  auto rd = std::make_shared<Ready>();

  rd->entries = raft->GetRaftLog()->UnstableEntries();
  rd->committed_entries = raft->GetRaftLog()->NextEnts();
  rd->messages = raft->Msgs();

  auto soft_st = raft->GetSoftState();
  if (!(soft_st == prev_soft_st)) {
    rd->soft_state = soft_st;
  }

  auto hard_st = raft->GetHardState();
  if (!(hard_st == prev_hard_st)) {
    rd->hard_state = hard_st;
  }

  if (raft->GetRaftLog()->UnstableSnapshot()) {
    rd->snapshot = raft->GetRaftLog()->UnstableSnapshot();
  } else {
    rd->snapshot = std::make_shared<raftpb::Snapshot>();
  }

  if (!raft->GetReadStates().empty()) {
    rd->read_states = raft->GetReadStates();
  }

  rd->must_sync = MustSync(raft->GetHardState(), prev_hard_st, rd->entries.size());
  return rd;
}

}  // namespace craft