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
#include <functional>
#include <map>
#include <memory>
#include <set>

#include "quorum/joint.h"
#include "raftpb/raft.pb.h"
#include "tracker/progress.h"

namespace craft {

using ProgressMap = std::map<uint64_t, ProgressPtr>;

ProgressPtr GetProgress(ProgressMap& prs, uint64_t id);
const ProgressPtr GetProgress(const ProgressMap& prs, uint64_t id);

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
class ProgressTracker {
 public:
  // Config reflects the configuration tracked in a ProgressTracker.
  struct Config {
    JointConfig voters_;
    // AutoLeave is true if the configuration is joint and a transition to the
    // incoming configuration should be carried out automatically by Raft when
    // this is possible. If false, the configuration will be joint until the
    // application initiates the transition manually.
    bool auto_leave_;
    // Learners is a set of IDs corresponding to the learners active in the
    // current configuration.
    //
    // Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    // either half of the joint config, it can't be a learner; if it is a
    // learner it can't be in either half of the joint config. This invariant
    // simplifies the implementation since it allows peers to have clarity about
    // its current role without taking into account joint consensus.
    std::set<uint64_t> learners_;
    // When we turn a voter into a learner during a joint consensus transition,
    // we cannot add the learner directly when entering the joint state. This is
    // because this would violate the invariant that the intersection of
    // voters and learners is empty. For example, assume a Voter is removed and
    // immediately re-added as a learner (or in other words, it is demoted):
    //
    // Initially, the configuration will be
    //
    //   voters:   {1 2 3}
    //   learners: {}
    //
    // and we want to demote 3. Entering the joint configuration, we naively get
    //
    //   voters:   {1 2} & {1 2 3}
    //   learners: {3}
    //
    // but this violates the invariant (3 is both voter and learner). Instead,
    // we get
    //
    //   voters:   {1 2} & {1 2 3}
    //   learners: {}
    //   next_learners: {3}
    //
    // Where 3 is now still purely a voter, but we are remembering the intention
    // to make it a learner upon transitioning into the final configuration:
    //
    //   voters:   {1 2}
    //   learners: {3}
    //   next_learners: {}
    //
    // Note that next_learners is not used while adding a learner that is not
    // also a voter in the joint config. In this case, the learner is added
    // right away when entering the joint configuration, so that it is caught up
    // as soon as possible.
    std::set<uint64_t> learners_next_;

    bool operator==(const Config& other) const {
      return voters_.Incoming().IDs() == other.voters_.Incoming().IDs() &&
        voters_.Outgoing().IDs() == other.voters_.Outgoing().IDs() &&
        auto_leave_ == other.auto_leave_ &&
        learners_ == other.learners_ &&
        learners_next_ == other.learners_next_;
    }

    bool Joint() const { return voters_.Outgoing().Size() > 0; }
    std::string Describe() const { return ""; }
  };

  using Closure = std::function<void(uint64_t id, ProgressPtr& pr)>;

  ProgressTracker(int64_t max_inflight) : max_inflight_(max_inflight) {}

  // ConfState returns a ConfState representing the active configuration.
  raftpb::ConfState ConfState();

  // IsSingleton returns true if (and only if) there is only one voting member
  // (i.e. the leader) in the current configuration.
  bool IsSingleton() {
    return config_.voters_.At(0).Size() == 1 &&
           config_.voters_.At(1).Size() == 0;
  }

  // Committed returns the largest log index known to be committed based on what
  // the voting members of the group have acknowledged.
  uint64_t Committed() const;

  // Visit invokes the supplied closure for all tracked progresses in stable
  // order.
  void Visit(Closure&& func);

  // QuorumActive returns true if the quorum is active from the view of the
  // local raft state machine. Otherwise, it returns false.
  bool QuorumActive();

  // VoterNodes returns a sorted slice of voters.
  std::vector<uint64_t> VoterNodes();

  // LearnerNodes returns a sorted slice of learners.
  std::vector<uint64_t> LearnerNodes();

  // ResetVotes prepares for a new round of vote counting via recordVote.
  void ResetVotes() { votes_.clear(); }

  // RecordVote records that the node with the given id voted for this Raft
  // instance if v == true (and declined it otherwise).
  void RecordVote(uint64_t id, bool v);

  // TallyVotes returns the number of granted and rejected Votes, and whether
  // the election outcome is known.
  std::tuple<int32_t, int32_t, VoteState> TallyVotes() const;

  const Config& GetConfig() const { return config_; }
  Config& GetConfig() { return config_; }
  const ProgressMap& GetProgressMap() const { return progress_; }
  ProgressPtr GetProgress(uint64_t id);

  void SetConfig(const Config& config) { config_ = config; }
  void SetConfig(Config&& config) { config_ = std::move(config); }
  void SetProgressMap(const ProgressMap& progress) {
    progress_ = progress;
  }
  void SetProgressMap(ProgressMap&& progress) {
    progress_ = std::move(progress);
  }

  int64_t MaxInflight() const { return max_inflight_; }

 private:
  Config config_;
  ProgressMap progress_;
  std::map<uint64_t, bool> votes_;
  int64_t max_inflight_;
};

}  // namespace craft