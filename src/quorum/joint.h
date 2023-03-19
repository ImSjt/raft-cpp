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

#include <map>
#include <set>
#include <string>

#include "quorum/majority.h"

namespace craft {

// JointConfig is a configuration of two groups of (possibly overlapping)
// majority configurations. Decisions require the support of both majorities.
class JointConfig {
 public:
  JointConfig() = default;
  JointConfig(const std::set<uint64_t>& c1, const std::set<uint64_t>& c2)
      : configs_{c1, c2} {}
  JointConfig(std::set<uint64_t>&& c1, std::set<uint64_t>&& c2)
      : configs_{std::move(c1), std::move(c2)} {}

  MajorityConfig& At(size_t i) {
    if (i < 0 || i >= sizeof(configs_)) {
      return configs_[0];
    }
    return configs_[i];
  }

  // IDs returns a newly initialized map representing the set of voters present
  // in the joint configuration.
  std::set<uint64_t> IDs() const;

  // CommittedIndex returns the largest committed index for the given joint
  // quorum. An index is jointly committed if it is committed in both
  // constituent majorities.
  Index CommittedIndex(const AckedIndexer& l) const;
  Index CommittedIndex(AckedIndexer&& l) const { return CommittedIndex(l); }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending, lost, or won. A
  // joint quorum requires both majority quorums to vote in favor.
  VoteState VoteResult(const std::map<uint64_t, bool>& votes) const;

  std::string String() const {
    if (configs_[1].Size() > 0) {
      return configs_[0].String() + "&&" + configs_[1].String();
    }
    return configs_[0].String();
  }

  // Describe returns a (multi-line) representation of the commit indexes for the
  // given lookuper.
  std::string Describe(const AckedIndexer& l) const {
    return MajorityConfig(IDs()).Describe(l);
  }

  MajorityConfig& Incoming() { return configs_[0]; }
  MajorityConfig& Outgoing() { return configs_[1]; }
  const MajorityConfig& Incoming() const { return configs_[0]; }
  const MajorityConfig& Outgoing() const { return configs_[1]; }

 private:
  MajorityConfig configs_[2];
};

}  // namespace craft