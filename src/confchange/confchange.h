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
#include <vector>

#include "raft.pb.h"
#include "status.h"
#include "tracker/tracker.h"

namespace craft {

// Changer facilitates configuration changes. It exposes methods to handle
// simple and joint consensus while performing the proper validation that allows
// refusing invalid configuration changes before they affect the active
// configuration.
class Changer {
 public:
  // Changer();

  // EnterJoint verifies that the outgoing (=right) majority config of the joint
  // config is empty and initializes it with a copy of the incoming (=left)
  // majority config. That is, it transitions from
  //
  //     (1 2 3)&&()
  // to
  //     (1 2 3)&&(1 2 3).
  //
  // The supplied changes are then applied to the incoming majority config,
  // resulting in a joint configuration that in terms of the Raft thesis[1]
  // (Section 4.3) corresponds to `C_{new,old}`.
  //
  // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
  std::tuple<ProgressTracker::Config, ProgressMap, Status> EnterJoint(
      bool auto_leave, const std::vector<raftpb::ConfChangeSingle>& ccs);

  // LeaveJoint transitions out of a joint configuration. It is an error to call
  // this method if the configuration is not joint, i.e. if the outgoing
  // majority config Voters[1] is empty.
  //
  // The outgoing majority config of the joint configuration will be removed,
  // that is, the incoming config is promoted as the sole decision maker. In the
  // notation of the Raft thesis[1] (Section 4.3), this method transitions from
  // `C_{new,old}` into `C_new`.
  //
  // At the same time, any staged learners (LearnersNext) the addition of which
  // was held back by an overlapping voter in the former outgoing config will be
  // inserted into Learners.
  //
  // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
  std::tuple<ProgressTracker::Config, ProgressMap, Status> LeaveJoint();

  // Simple carries out a series of configuration changes that (in aggregate)
  // mutates the incoming majority config Voters[0] by at most one. This method
  // will return an error if that is not the case, if the resulting quorum is
  // zero, or if the configuration is in a joint state (i.e. if there is an
  // outgoing configuration).
  std::tuple<ProgressTracker::Config, ProgressMap, Status> Simple(
      const std::vector<raftpb::ConfChangeSingle>& ccs);

  // Apply a change to the configuration. By convention, changes to voters are
  // always made to the incoming majority config Voters[0]. Voters[1] is either
  // empty or preserves the outgoing majority configuration while in a joint
  // state.
  Status Apply(ProgressTracker::Config& cfg, ProgressMap& prs,
               const std::vector<raftpb::ConfChangeSingle>& ccs);

  // MakeVoter adds or promotes the given ID to be a voter in the incoming
  // majority config.
  void MakeVoter(ProgressTracker::Config& cfg, ProgressMap& prs, uint64_t id);

  // MakeLearner makes the given ID a learner or stages it to be a learner once
  // an active joint configuration is exited.
  //
  // The former happens when the peer is not a part of the outgoing config, in
  // which case we either add a new learner or demote a voter in the incoming
  // config.
  //
  // The latter case occurs when the configuration is joint and the peer is a
  // voter in the outgoing config. In that case, we do not want to add the peer
  // as a learner because then we'd have to track a peer as a voter and learner
  // simultaneously. Instead, we add the learner to LearnersNext, so that it
  // will be added to Learners the moment the outgoing config is removed by
  // LeaveJoint().
  void MakeLearner(ProgressTracker::Config& cfg, ProgressMap& prs, uint64_t id);

  // remove this peer as a voter or learner from the incoming config.
  void Remove(ProgressTracker::Config& cfg, ProgressMap& prs, uint64_t id);

  // initProgress initializes a new progress for the given node or learner.
  void InitProgress(ProgressTracker::Config& cfg, ProgressMap& prs, uint64_t id,
                    bool is_learner);

  ProgressTracker& GetProgressTracker() { return tracker_; }

 private:
  // checkInvariants makes sure that the config and progress are compatible with
  // each other. This is used to check both what the Changer is initialized
  // with, as well as what it returns.
  Status CheckInvariants(const ProgressTracker::Config& cfg,
                         const ProgressMap& prs) const;

  // CheckAndCopy copies the tracker's config and progress map (deeply enough
  // for the purposes of the Changer) and returns those copies. It returns an
  // error if checkInvariants does.
  std::tuple<ProgressTracker::Config, ProgressMap, Status> CheckAndCopy() const;

  // CheckAndReturn calls checkInvariants on the input and returns either the
  // resulting error or the input.
  std::tuple<ProgressTracker::Config, ProgressMap, Status> CheckAndReturn(
      ProgressTracker::Config&& cfg, ProgressMap&& prs) const;

  int Symdiff(const std::set<uint64_t>& l, const std::set<uint64_t>& r);

  ProgressTracker tracker_;
  uint64_t last_index_;
};

}  // namespace craft