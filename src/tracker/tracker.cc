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
#include "src/tracker/tracker.h"

#include "src/quorum/quorum.h"

namespace craft {

class MatchAckIndexer : public AckedIndexer {
 public:
  MatchAckIndexer(const ProgressMap* m)
      : progress_map_(m) {}

  std::tuple<Index, bool> AckedIndex(uint64_t voter_id) const {
    auto it = progress_map_->find(voter_id);
    if (it == progress_map_->end()) {
      return std::make_tuple(static_cast<Index>(0), false);
    }
    return std::make_tuple(static_cast<Index>(it->second->Match()), true);
  }

 private:
  const ProgressMap* progress_map_;
};

ProgressPtr GetProgress(ProgressMap& prs, uint64_t id) {
  auto it = prs.find(id);
  if (it == prs.end()) {
    return std::shared_ptr<Progress>();
  }
  return it->second;
}

const ProgressPtr GetProgress(const ProgressMap& prs, uint64_t id) {
  auto it = prs.find(id);
  if (it == prs.end()) {
    return std::shared_ptr<Progress>();
  }
  return it->second;
}

raftpb::ConfState ProgressTracker::ConfState() {
  raftpb::ConfState conf_state;

  std::vector<uint64_t> voters = config_.voters.Incoming().Slice();
  for (uint64_t voter : voters) {
    conf_state.add_voters(voter);
  }

  std::vector<uint64_t> voters_outgoing = config_.voters.Outgoing().Slice();
  for (uint64_t voter : voters_outgoing) {
    conf_state.add_voters_outgoing(voter);
  }

  for (uint64_t learner : config_.learners) {
    conf_state.add_learners(learner);
  }

  for (uint64_t learner : config_.learners_next) {
    conf_state.add_learners_next(learner);
  }

  conf_state.set_auto_leave(config_.auto_leave);

  return conf_state;
}

uint64_t ProgressTracker::Committed() const {
  return config_.voters.CommittedIndex(MatchAckIndexer(&progress_));
}

void ProgressTracker::Visit(Closure&& func) {
  for (auto& p : progress_) {
    func(p.first, p.second);
  }
}

bool ProgressTracker::QuorumActive() {
  std::map<uint64_t, bool> votes;
  Visit([&votes](uint64_t id, std::shared_ptr<Progress> pr) {
    if (pr->IsLearner()) {
      return;
    }
    votes[id] = pr->RecentActive();
  });

  return config_.voters.VoteResult(votes) == kVoteWon;
}

std::vector<uint64_t> ProgressTracker::VoterNodes() {
  std::set<uint64_t> m = config_.voters.IDs();
  std::vector<uint64_t> nodes;
  for (uint64_t n : m) {
    nodes.push_back(n);
  }
  return nodes;
}

std::vector<uint64_t> ProgressTracker::LearnerNodes() {
  if (config_.learners.empty()) {
    return {};
  }

  std::vector<uint64_t> nodes;
  for (uint64_t n : config_.learners) {
    nodes.push_back(n);
  }
  return nodes;
}

void ProgressTracker::RecordVote(uint64_t id, bool v) {
  auto it = votes_.find(id);
  if (it == votes_.end()) {
    votes_[id] = v;
  }
}

std::tuple<int32_t, int32_t, VoteState> ProgressTracker::TallyVotes() const {
  // Make sure to populate granted/rejected correctly even if the Votes slice
  // contains members no longer part of the configuration. This doesn't really
  // matter in the way the numbers are used (they're informational), but might
  // as well get it right.
  int32_t granted = 0;
  int32_t rejected = 0;
  for (const auto& p : progress_) {
    if (p.second->IsLearner()) {
      continue;
    }
    auto v = votes_.find(p.first);
    if (v == votes_.end()) {
      continue;
    }
    if (v->second) {
      granted++;
    } else {
      rejected++;
    }
  }
  return std::make_tuple(granted, rejected, config_.voters.VoteResult(votes_));
}

std::shared_ptr<Progress> ProgressTracker::GetProgress(uint64_t id) {
  return craft::GetProgress(progress_, id);
}

}  // namespace craft