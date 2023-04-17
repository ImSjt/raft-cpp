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
#include <optional>

#include "raft.h"
#include "raftpb/confchange.h"

namespace craft {

enum SnapshotStatus {
  kFinish = 1,
  kFailure = 2,
};

enum ProgressType {
  kPeer = 1,
  kLearner = 2,
};

class RawNode {
 public:
  using Visitor = std::function<void(uint64_t id, ProgressType type, ProgressPtr pr)>;

  static std::unique_ptr<RawNode> New(Raft::Config& c);

  RawNode(std::unique_ptr<Raft>&& raft) : raft_(std::move(raft)) {
    prev_soft_st_ = raft_->GetSoftState();
    prev_hard_st_ = raft_->GetHardState();
  }

  // Tick advances the internal logical clock by a single tick.
  void Tick();

  // TickQuiesced advances the internal logical clock by a single tick without
  // performing any other state machine processing. It allows the caller to avoid
  // periodic heartbeats and elections when all of the peers in a Raft group are
  // known to be at the same state. Expected usage is to periodically invoke Tick
  // or TickQuiesced depending on whether the group is "active" or "quiesced".
  //
  // WARNING: Be very careful about using this method as it subverts the Raft
  // state machine. You should probably be using Tick instead.
  void TickQuiesced();

  // Campaign causes this RawNode to transition to candidate state.
  Status Campaign();

  // Propose proposes data be appended to the raft log.
  Status Propose(const std::string& data);

  // ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
  // details.
  Status ProposeConfChange(const ConfChangeI& cc);

  // ApplyConfChange applies a config change to the local node. The app must call
  // this when it applies a configuration change, except when it decides to reject
  // the configuration change, in which case no call must take place.
  raftpb::ConfState ApplyConfChange(const ConfChangeI& cc);

  // Step advances the state machine using the given message.
  Status Step(MsgPtr m);

  // GetReady returns the outstanding work that the application needs to handle. This
  // includes appending and applying entries or a snapshot, updating the HardState,
  // and sending messages. The returned Ready() *must* be handled and subsequently
  // passed back via Advance().
  Ready GetReady();

  // ReadyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
  // is no obligation that the Ready must be handled.
  Ready ReadyWithoutAccept();

  // acceptReady is called when the consumer of the RawNode has decided to go
  // ahead and handle a Ready. Nothing must alter the state of the RawNode between
  // this call and the prior call to Ready().
  void AcceptReady(const Ready& rd);

  // HasReady called when RawNode user need to check if any Ready pending.
  // Checking logic in this method should be consistent with Ready.containsUpdates().
  bool HasReady() const;

  // Advance notifies the RawNode that the application has applied and saved progress in the
  // last Ready results.
  void Advance(const Ready& rd);

  // // GetStatus returns the current status of the given group. This allocates, see
  // // BasicStatus and WithProgress for allocation-friendlier choices.
  // Status GetStatus() const;

  // // BasicStatus returns a BasicStatus. Notably this does not contain the
  // // Progress map; see WithProgress for an allocation-free way to inspect it.
  // BasicStatus GetBasicStatus() const;

  // WithProgress is a helper to introspect the Progress for this node and its
  // peers.
  void WithProgress(Visitor&& visitor);

  // ReportUnreachable reports the given node is not reachable for the last send.
  void ReportUnreachable(uint64_t id);

  // ReportSnapshot reports the status of the sent snapshot.
  void ReportSnapshot(uint64_t id, SnapshotStatus status);

  // TransferLeader tries to transfer leadership to the given transferee.
  void TransferLeader(uint64_t transferee);

  // ReadIndex requests a read state. The read state will be set in ready.
  // Read State has a read index. Once the application advances further than the read
  // index, any linearizable read requests issued before the read request can be
  // processed safely. The read state will have the same rctx attached.
  void ReadIndex(const std::string& rctx);

  // void SetPreSoftState(const SoftState& state) {
  //   pre_soft_st_ = state;
  // }
  // void SetPreHardState(const raftpb::HardState& state) {
  //   pre_hard_st_ = state;
  // }

 private:
  std::unique_ptr<Raft> raft_;
  std::optional<SoftState> prev_soft_st_;
  std::optional<raftpb::HardState> prev_hard_st_;
};

}  // namespace craft