// Copyright 2023 juntaosu
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
#include "src/confchange/confchange.h"

namespace craft {

static std::tuple<ProgressTracker::Config, ProgressMap, Status> err(Status&& status) {
  return std::make_tuple(ProgressTracker::Config(), ProgressMap(), std::move(status)); 
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Changer::EnterJoint(
    bool auto_leave, const std::vector<raftpb::ConfChangeSingle>& ccs) const {
  auto [cfg, prs, status] = CheckAndCopy();
  if (!status.IsOK()) {
    return err(std::move(status));
  }

  if (cfg.Joint()) {
    return err(Status::Error("config is already joint"));
  }

  if (cfg.voters.Incoming().Size() == 0) {
		// We allow adding nodes to an empty config for convenience (testing and
		// bootstrap), but you can't enter a joint state.
    return err(Status::Error("can't make a zero-voter config joint"));
  }

  // Clear the outgoing config.
  cfg.voters.Outgoing().Reset();
  // Copy incoming to outgoing.
  cfg.voters.Outgoing() = cfg.voters.Incoming();

  status = Apply(cfg, prs, ccs);
  if (!status.IsOK()) {
    return err(std::move(status));
  }
  cfg.auto_leave = auto_leave;
  return CheckAndReturn(std::move(cfg), std::move(prs));
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Changer::LeaveJoint() const {
  auto [cfg, prs, status] = CheckAndCopy();
  if (!status.IsOK()) {
    return err(std::move(status));
  }
  if (!cfg.Joint()) {
    return err(Status::Error("can't leave a non-joint config"));
  }
  if (cfg.voters.Outgoing().Size() == 0) {
    return err(Status::Error("configuration is not joint: %s", cfg.String().c_str()));
  }
  for (auto id : cfg.learners_next) {
    cfg.learners.insert(id);
    GetProgress(prs, id)->SetIsLearner(true);
  }
  cfg.learners_next.clear();

  for (auto id : cfg.voters.Outgoing().IDs()) {
    bool is_voter = cfg.voters.Incoming().Exist(id);
    bool is_learner = cfg.learners.count(id) != 0;
    if (!is_voter && !is_learner) {
      prs.erase(id);
    }
  }
  cfg.voters.Outgoing().Reset();
  cfg.auto_leave = false;

  return CheckAndReturn(std::move(cfg), std::move(prs));
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Changer::Simple(
    const std::vector<raftpb::ConfChangeSingle>& ccs) const {
  auto [cfg, prs, status] = CheckAndCopy();
  if (!status.IsOK()) {
    return err(std::move(status));
  }
  if (cfg.Joint()) {
    return err(Status::Error("can't apply simple config change in joint config"));
  }
  status = Apply(cfg, prs, ccs);
  if (!status.IsOK()) {
    return err(std::move(status));
  }
  auto n = Symdiff(tracker_.GetConfig().voters.Incoming().IDs(), cfg.voters.Incoming().IDs());
  if (n > 1) {
    return err(Status::Error("more than one voter changed without entering joint config"));
  }
  return CheckAndReturn(std::move(cfg), std::move(prs));
}

Status Changer::Apply(ProgressTracker::Config& cfg, ProgressMap& prs,
                      const std::vector<raftpb::ConfChangeSingle>& ccs) const {
  for (auto cc : ccs) {
    if (cc.node_id() == 0) {
			// etcd replaces the NodeID with zero if it decides (downstream of
			// raft) to not apply a change, so we have to have explicit code
			// here to ignore these.
      continue;
    }
    switch (cc.type()) {
    case raftpb::ConfChangeType::ConfChangeAddNode:
      MakeVoter(cfg, prs, cc.node_id());
      break;
    case raftpb::ConfChangeType::ConfChangeAddLearnerNode:
      MakeLearner(cfg, prs, cc.node_id());
      break;
    case raftpb::ConfChangeType::ConfChangeRemoveNode:
      Remove(cfg, prs, cc.node_id());
      break;
    case raftpb::ConfChangeType::ConfChangeUpdateNode:
      break;
    default:
      return Status::Error("unexpected conf type %d", cc.type());
    }
  }
  if (cfg.voters.Incoming().Size() == 0) {
    return Status::Error("removed all voters");
  }
  return Status::OK();
}

void Changer::MakeVoter(ProgressTracker::Config& cfg, ProgressMap& prs,
                        uint64_t id) const {
  auto pr = GetProgress(prs, id);
  if (!pr) {
    InitProgress(cfg, prs, id, false);
    return;
  }
  pr->SetIsLearner(false);
  cfg.learners.erase(id);
  cfg.learners_next.erase(id);
  cfg.voters.Incoming().Add(id);
}

void Changer::MakeLearner(ProgressTracker::Config& cfg, ProgressMap& prs,
                          uint64_t id) const {
  auto pr = GetProgress(prs, id);
  if (!pr) {
    InitProgress(cfg, prs, id, true);
    return;
  }
  if (pr->IsLearner()) {
    return;
  }
	// Remove any existing voter in the incoming config...
  Remove(cfg, prs, id);
	// ... but save the Progress.
  prs[id] = pr;
	// Use LearnersNext if we can't add the learner to Learners directly, i.e.
	// if the peer is still tracked as a voter in the outgoing config. It will
	// be turned into a learner in LeaveJoint().
	//
	// Otherwise, add a regular learner right away.
  if (cfg.voters.Outgoing().Exist(id)) {
    cfg.learners_next.insert(id);
  } else {
    pr->SetIsLearner(true);
    cfg.learners.insert(id);
  }
}

void Changer::Remove(ProgressTracker::Config& cfg, ProgressMap& prs,
                     uint64_t id) const {
  auto pr = GetProgress(prs, id);
  if (!pr) {
    return;
  }
  cfg.voters.Incoming().Remove(id);
  cfg.learners.erase(id);
  cfg.learners_next.erase(id);

	// If the peer is still a voter in the outgoing config, keep the Progress.
  if (!cfg.voters.Outgoing().Exist(id)) {
    prs.erase(id);
  }
}

void Changer::InitProgress(ProgressTracker::Config& cfg, ProgressMap& prs,
                           uint64_t id, bool is_learner) const {
  if (!is_learner) {
    cfg.voters.Incoming().Add(id);
  } else {
    cfg.learners.insert(id);
  }

  prs[id] = std::make_shared<Progress>(
      logger_,
      // Initializing the Progress with the last index means that the follower
      // can be probed (with the last index).
      //
      // TODO(tbg): seems awfully optimistic. Using the first index would be
      // better. The general expectation here is that the follower has no log
      // at all (and will thus likely need a snapshot), though the app may
      // have applied a snapshot out of band before adding the replica (thus
      // making the first index the better choice).
      last_index_,
      0,
      tracker_.MaxInflight(),
      is_learner,
      // When a node is first added, we should mark it as recently active.
      // Otherwise, CheckQuorum may cause us to step down if it is invoked
      // before the added node has had a chance to communicate with us.
      true);
}

Status Changer::CheckInvariants(const ProgressTracker::Config& cfg,
                                const ProgressMap& prs) const {
	// NB: intentionally allow the empty config. In production we'll never see a
	// non-empty config (we prevent it from being created) but we will need to
	// be able to *create* an initial config, for example during bootstrap (or
	// during tests). Instead of having to hand-code this, we allow
	// transitioning from an empty config into any other legal and non-empty
	// config.
  auto check_exist = [&prs](const std::set<uint64_t>& ids) {
    for (auto id : ids) {
      auto it = prs.find(id);
      if (it == prs.end()) {
        return Status::Error("no progress for %d", id);
      }
    }
    return Status::OK();
  };
  auto status = check_exist(cfg.voters.IDs());
  if (!status.IsOK()) {
    return status;
  }
  status = check_exist(cfg.learners);
  if (!status.IsOK()) {
    return status;
  }
  status = check_exist(cfg.learners_next);
  if (!status.IsOK()) {
    return status;
  }

	// Any staged learner was staged because it could not be directly added due
	// to a conflicting voter in the outgoing config.
  for (auto id : cfg.learners_next) {
    if (!cfg.voters.Outgoing().Exist(id)) {
      return Status::Error("%d is in LearnersNext, but not Voters[1]", id);
    }
    if (GetProgress(prs, id)->IsLearner()) {
      return Status::Error("%d is in LearnersNext, but is already marked as learner", id);
    }
  }

	// Conversely Learners and Voters doesn't intersect at all.
  for (auto id : cfg.learners) {
    if (cfg.voters.Outgoing().Exist(id)) {
      return Status::Error("%d is in Learners and Voters[1]", id);
    }
    if (cfg.voters.Incoming().Exist(id)) {
      return Status::Error("%d is in Learners and Voters[0]", id);
    }
    if (!GetProgress(prs, id)->IsLearner()) {
      return Status::Error("%d is in Learners, but is not marked as learner", id);
    }
  }

  if (!cfg.Joint()) {
    if (cfg.voters.Outgoing().Size() > 0) {
      return Status::Error("cfg.Voters[1] must be nil when not joint");
    }
    if (!cfg.learners_next.empty()) {
      return Status::Error("cfg.LearnersNext must be nil when not joint");
    }
    if (cfg.auto_leave) {
      return Status::Error("AutoLeave must be false when not joint");
    }
  }

  return Status::OK();
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Changer::CheckAndCopy() const {
  auto cfg = tracker_.GetConfig();
  auto prs = tracker_.GetProgressMap();
  ProgressMap prs_copy;
  for (auto& p : prs) {
    prs_copy[p.first] = p.second->Clone();
  }
  return CheckAndReturn(std::move(cfg), std::move(prs_copy));
}

std::tuple<ProgressTracker::Config, ProgressMap, Status> Changer::CheckAndReturn(
    ProgressTracker::Config&& cfg, ProgressMap&& prs) const {
  auto status = CheckInvariants(cfg, prs);
  if (!status.IsOK()) {
    return err(std::move(status));
  }
  return std::make_tuple(std::move(cfg), std::move(prs), Status::OK());
}

int Changer::Symdiff(const std::set<uint64_t>& l, const std::set<uint64_t>& r) const {
  int n = 0;
  // count elems in l but not in r
  for (auto id : l) {
    if (r.count(id) == 0) {
      n++;
    }
  }
  for (auto id : r) {
    if (l.count(id) == 0) {
      n++;
    }
  }
  return n;
}

}  // namespace craft