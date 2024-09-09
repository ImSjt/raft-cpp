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
#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "src/quorum/quorum.h"

namespace craft {

class JointConfig;

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
class MajorityConfig {
 public:
  MajorityConfig() = default;
  MajorityConfig(const std::set<uint64_t> &c) : config_(c) {}
  MajorityConfig(std::set<uint64_t> &&c) : config_(std::move(c)) {}

  size_t Size() const { return config_.size(); }

  void Reset() { config_.clear(); }

  bool Exist(uint64_t id) const { return config_.count(id) != 0; }

  void Add(uint64_t id) { config_.insert(id); }

  void Remove(uint64_t id) { config_.erase(id); }

  const std::set<uint64_t>& IDs() const { return config_; }

  // Slice returns the MajorityConfig as a sorted slice.
  std::vector<uint64_t> Slice() const;

  // CommittedIndex computes the committed index from those supplied via the
  // provided AckedIndexer (for the active config).
  Index CommittedIndex(const AckedIndexer& l) const;
  // uint64_t CommittedIndex(AckedIndexer &&l) { return CommittedIndex(l); }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending (i.e. neither a
  // quorum of yes/no has been reached), won (a quorum of yes has been reached),
  // or lost (a quorum of no has been reached).
  VoteState VoteResult(const std::map<uint64_t, bool>& votes) const;

  void IDs(std::set<uint64_t>& ids) const;

  std::string String() const;
  // Describe returns a (multi-line) representation of the commit indexes for the
  // given lookuper.
  std::string Describe(const AckedIndexer& l) const;

 private:
  friend class JointConfig;
  // The node set of the current configuration
  std::set<uint64_t> config_;
};

}  // namespace craft