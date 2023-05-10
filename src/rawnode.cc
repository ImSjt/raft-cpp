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
#include "rawnode.h"
#include "util.h"

namespace craft {

// ErrStepLocalMsg is returned when try to step a local raft message
static const char* kErrStepLocalMsg = "raft: cannot step raft local message";

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.prs for that node.
static const char* kErrStepPeerNotFound = "raft: cannot step as peer not found";

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

bool operator==(const SoftState& s1, const SoftState& s2) {
  return s1.lead == s2.lead && s1.raft_state == s2.raft_state;
}

bool operator==(const raftpb::HardState& s1, const raftpb::HardState& s2) {
  return s1.term() == s2.term() && s1.vote() == s2.vote() && s1.commit() == s2.commit();
}

std::unique_ptr<RawNode> RawNode::New(Raft::Config& c) {
  auto raft = Raft::New(c);
  auto rn = std::make_unique<RawNode>(std::move(raft));
  return std::move(rn);
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

Ready RawNode::GetReady() {
  auto rd = ReadyWithoutAccept();
  AcceptReady(rd);
  return rd;
}

Ready RawNode::ReadyWithoutAccept() {
  // Ready rd;
  // rd.entries = raft_->GetRaftLog()->UnstableEntries();
  // rd.committed_entries = raft_->GetRaftLog()->NextEnts();
  // rd.messages = raft_->Msgs();

  // auto soft_st = raft_->GetSoftState();
  // if (soft_st != *prev_soft_st_) {
  //   rd.soft_state = soft_st;
  // }

  // auto hard_st = raft_->GetHardState();
  // if (hard_st != *prev_hard_st_) {
  //   rd.hard_state = hard_st;
  // }

  // if (raft_->GetRaftLog()->UnstableSnapshot()) {
  //   rd.snapshot = raft_->GetRaftLog()->UnstableSnapshot();
  // }

  // if (!raft_->GetReadStates().empty()) {
  //   rd.read_states = raft_->GetReadStates();
  // }

  // rd.must_sync = MustSync(raft_->GetHardState(), *prev_hard_st_, rd.entries.size());
  // return rd;
  return NewReady(raft_.get(), prev_soft_st_, prev_hard_st_);
}

void RawNode::AcceptReady(const Ready& rd) {
  if (rd.soft_state.has_value()) {
    prev_soft_st_ = *(rd.soft_state);
  }
  if (!rd.read_states.empty()) {
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

void RawNode::Advance(const Ready& rd) {
  if (!IsEmptyHardState(rd.hard_state)) {
    prev_hard_st_ = rd.hard_state;
  }
  raft_->Advance(rd);
}

// // GetStatus returns the current status of the given group. This allocates, see
// // BasicStatus and WithProgress for allocation-friendlier choices.
// Status GetStatus() const;

// // BasicStatus returns a BasicStatus. Notably this does not contain the
// // Progress map; see WithProgress for an allocation-free way to inspect it.
// BasicStatus GetBasicStatus() const;

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

Ready NewReady(Raft* raft, const SoftState& prev_soft_st, const raftpb::HardState& prev_hard_st) {
  Ready rd;
  rd.entries = raft->GetRaftLog()->UnstableEntries();
  rd.committed_entries = raft->GetRaftLog()->NextEnts();
  rd.messages = raft->Msgs();

  auto soft_st = raft->GetSoftState();
  if (!(soft_st == prev_soft_st)) {
    rd.soft_state = soft_st;
  }

  auto hard_st = raft->GetHardState();
  if (!(hard_st == prev_hard_st)) {
    rd.hard_state = hard_st;
  }

  if (raft->GetRaftLog()->UnstableSnapshot()) {
    rd.snapshot = raft->GetRaftLog()->UnstableSnapshot();
  }

  if (!raft->GetReadStates().empty()) {
    rd.read_states = raft->GetReadStates();
  }

  rd.must_sync = MustSync(raft->GetHardState(), prev_hard_st, rd.entries.size());
  return rd;
}

}  // namespace craft