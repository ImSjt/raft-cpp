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
#include "confchange/restore.h"

#include <functional>

namespace craft {

using ChangeOp = std::function<std::tuple<ProgressTracker::Config, ProgressMap, Status> (Changer&)>;

// ToConfChangeSingle translates a conf state into 1) a slice of operations creating
// first the config that will become the outgoing one, and then the incoming one, and
// b) another slice that, when applied to the config resulted from 1), represents the
// ConfState.
static std::tuple<std::vector<raftpb::ConfChangeSingle>, std::vector<raftpb::ConfChangeSingle>>
    toConfChangeSingle(const raftpb::ConfState& cs) {
  std::vector<raftpb::ConfChangeSingle> outgoing;
  std::vector<raftpb::ConfChangeSingle> incoming;
	// Example to follow along this code:
	// voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
	//
	// This means that before entering the joint config, the configuration
	// had voters (1 2 4 6) and perhaps some learners that are already gone.
	// The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
	// are no longer voters; however 4 is poised to become a learner upon leaving
	// the joint state.
	// We can't tell whether 5 was a learner before entering the joint config,
	// but it doesn't matter (we'll pretend that it wasn't).
	//
	// The code below will construct
	// outgoing = add 1; add 2; add 4; add 6
	// incoming = remove 1; remove 2; remove 4; remove 6
	//            add 1;    add 2;    add 3;
	//            add-learner 5;
	//            add-learner 4;
	//
	// So, when starting with an empty config, after applying 'outgoing' we have
	//
	//   quorum=(1 2 4 6)
	//
	// From which we enter a joint state via 'incoming'
	//
	//   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
	//
	// as desired.
  for (auto id : cs.voters_outgoing()) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(id);
    outgoing.emplace_back(std::move(cc));
  }

	// We're done constructing the outgoing slice, now on to the incoming one
	// (which will apply on top of the config created by the outgoing slice).

	// First, we'll remove all of the outgoing voters.
  for (auto id : cs.voters_outgoing()) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeRemoveNode);
    cc.set_node_id(id);
    incoming.emplace_back(std::move(cc));
  }
	// Then we'll add the incoming voters and learners.
  for (auto id : cs.voters()) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(id);
    incoming.emplace_back(std::move(cc));
  }
  for (auto id : cs.learners()) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    cc.set_node_id(id);
    incoming.emplace_back(std::move(cc));
  }
	// Same for LearnersNext; these are nodes we want to be learners but which
	// are currently voters in the outgoing config.
  for (auto id : cs.learners_next()) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    cc.set_node_id(id);
    incoming.emplace_back(std::move(cc));
  }
  return std::make_tuple(std::move(outgoing), std::move(incoming));
}

static std::tuple<ProgressTracker::Config, ProgressMap, Status> chain(
    Changer& chg, const std::vector<ChangeOp>& ops) {
  for (auto& op : ops) {
    auto [cfg, prs, status] = op(chg);
    if (!status.IsOK()) {
      return std::make_tuple(ProgressTracker::Config(), ProgressMap(), std::move(status));
    }
    chg.GetProgressTracker().SetConfig(std::move(cfg));
    chg.GetProgressTracker().SetProgressMap(std::move(prs));
  }
  return std::make_tuple(chg.GetProgressTracker().GetConfig(),
                         chg.GetProgressTracker().GetProgressMap(),
                         Status::OK());
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Restore(
    Changer chg, const raftpb::ConfState& cs) {
  auto [outgoing, incoming] = toConfChangeSingle(cs);

  std::vector<ChangeOp> ops;

  if (outgoing.size() == 0) {
		// No outgoing config, so just apply the incoming changes one by one.
    for (auto& cc : incoming) {
      std::vector<raftpb::ConfChangeSingle> ccs{cc};
      ops.emplace_back([ccs = std::move(ccs)](Changer& chg) {
        return chg.Simple(ccs);
      });
    }
  } else {
		// The ConfState describes a joint configuration.
		//
		// First, apply all of the changes of the outgoing config one by one, so
		// that it temporarily becomes the incoming active config. For example,
		// if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
    for (auto& cc : outgoing) {
      std::vector<raftpb::ConfChangeSingle> ccs{cc};
      ops.emplace_back([ccs = std::move(ccs)](Changer& chg) {
        return chg.Simple(ccs);
      });
    }
		// Now enter the joint state, which rotates the above additions into the
		// outgoing config, and adds the incoming config in. Continuing the
		// example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
		// would be removing 2,3,4 and then adding in 1,2,3 while transitioning
		// into a joint state.
    ops.emplace_back([&cs, incoming = std::move(incoming)](Changer& chg) {
      return chg.EnterJoint(cs.auto_leave(), incoming);
    });
  }
  return chain(chg, ops);
}

}  // namespace craft