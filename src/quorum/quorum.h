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
#include <map>

namespace craft {

using Index = uint64_t;

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
class AckedIndexer {
 public:
  virtual ~AckedIndexer() = default;
  virtual std::tuple<Index, bool> AckedIndex(uint64_t voter_id) const = 0;
};

class MapAckIndexer : public AckedIndexer {
 public:
  MapAckIndexer(const std::map<uint64_t, Index> &indexer)
      : map_ack_indexer_(indexer) {}
  MapAckIndexer(std::map<uint64_t, Index> &&indexer)
      : map_ack_indexer_(std::move(indexer)) {}

  std::tuple<Index, bool> AckedIndex(uint64_t voter_id) const {
    auto it = map_ack_indexer_.find(voter_id);
    if (it == map_ack_indexer_.end()) {
      return std::make_tuple(static_cast<Index>(0), false);
    }
    return std::make_tuple(it->second, true);
  }

 private:
  // voter_id -> commit_index
  std::map<uint64_t, Index> map_ack_indexer_;
};

enum VoteState : uint8_t {
  // VotePending indicates that the decision of the vote depends on future
  // votes, i.e. neither "yes" or "no" has reached quorum yet.
  kVotePending,
  // VoteLost indicates that the quorum has voted "no".
  kVoteLost,
  // VoteWon indicates that the quorum has voted "yes".
  kVoteWon
};

}  // namespace craft