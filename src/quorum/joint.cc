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
#include "src/quorum/joint.h"

namespace craft {

std::set<uint64_t> JointConfig::IDs() const {
  std::set<uint64_t> ids;
  for (auto& config : configs_) {
    config.IDs(ids);
  }
  return ids;
}

Index JointConfig::CommittedIndex(const AckedIndexer& l) const {
  Index idx0 = configs_[0].CommittedIndex(l);
  Index idx1 = configs_[1].CommittedIndex(l);
  return std::min(idx0, idx1);
}

VoteState JointConfig::VoteResult(const std::map<uint64_t, bool>& votes) const {
  VoteState r1 = configs_[0].VoteResult(votes);
  VoteState r2 = configs_[1].VoteResult(votes);

  if (r1 == r2) {
    // If they agree, return the agreed state.
    return r1;
  }
  if (r1 == kVoteLost || r2 == kVoteLost) {
    // If either config has lost, loss is the only possible outcome.
    return kVoteLost;
  }
  // One side won, the other one is pending, so the whole outcome is.
  return kVotePending;
}

}  // namespace craft
