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
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "gtest/gtest.h"
#include "src/quorum/majority.h"
#include "src/quorum/joint.h"

static craft::Index alternativeMajorityCommittedIndex(
    const craft::MajorityConfig& c, const craft::AckedIndexer& l) {
  if (c.Size() == 0) {
    return UINT64_MAX;
  }

  std::map<uint64_t, craft::Index> id_to_idx;
  for (auto id : c.Slice()) {
    auto [idx, ok] = l.AckedIndex(id);
    if (ok) {
      id_to_idx[id] = idx;
    }
  }

	// Build a map from index to voters who have acked that or any higher index.
  std::map<craft::Index, int> idx_to_votes;
  for (auto& p : id_to_idx) {
    idx_to_votes[p.second] = 0;
  }

  for (auto& p1 : id_to_idx) {
    for (auto& p2 : idx_to_votes) {
      if (p2.first > p1.second) {
        continue;
      }
      p2.second++;
    }
  }

	// Find the maximum index that has achieved quorum.
  auto q = c.Size() / 2 + 1;
  craft::Index max_quorum_idx = 0;
  for (auto& p : idx_to_votes) {
    if (p.second >= static_cast<int>(q) && p.first > max_quorum_idx) {
      max_quorum_idx = p.first;
    }
  }
  return max_quorum_idx;
}

TEST(MajorityConfig, CommittedIndex) {
  struct Test {
    std::string name;
    std::set<uint64_t> cfg;
    std::map<uint64_t, craft::Index> idx;
    uint64_t windex;
  };

  std::vector<Test> tests = {
    // The empty quorum commits "everything". This is useful for its use in joint quorums.
    {"test0", {}, {}, UINT64_MAX},
    // A single voter quorum is not final when no index is known.
    {"test1", {1}, {}, 0},
    // When an index is known, that's the committed index, and that's final.
    {"test2", {1}, {{1, 12}}, 12},
    // With two nodes, start out similarly.
    {"test3", {1, 2}, {}, 0},
    // The first committed index becomes known (for n1). Nothing changes in the
    // output because idx=12 is not known to be on a quorum (which is both nodes).
    {"test4", {1, 2}, {{1, 12}}, 0},
    // The second index comes in and finalize the decision. The result will be the
    // smaller of the two indexes.
    {"test5", {1, 2}, {{1, 12}, {2, 5}}, 5},
    // No surprises for three nodes.
    {"test6", {1, 2, 3}, {}, 0},
    {"test7", {1, 2, 3}, {{1, 12}}, 0},
    // We see a committed index, but a higher committed index for the last pending
    // votes could change (increment) the outcome, so not final yet.
    {"test8", {1, 2, 3}, {{1, 12}, {2, 5}}, 5},
    // a) the case in which it does:
    {"test9", {1, 2, 3}, {{1, 12}, {2, 5}, {3, 6}}, 6},
    // b) the case in which it does not:
    {"test10", {1, 2, 3}, {{1, 12}, {2, 5}, {3, 4}}, 5},
    // c) a different case in which the last index is pending but it has no chance of
    // swaying the outcome (because nobody in the current quorum agrees on anything
    // higher than the candidate):
    {"test11", {1, 2, 3}, {{1, 5}, {2, 5}}, 5},
    // c) continued: Doesn't matter what shows up last. The result is final.
    {"test12", {1, 2, 3}, {{1, 5}, {2, 5}, {3, 12}}, 5},
    // With all committed idx known, the result is final.
    {"test13", {1, 2, 3}, {{1, 100}, {2, 101}, {3, 103}}, 101},
    // Some more complicated examples. Similar to case c) above. The result is
    // already final because no index higher than 103 is one short of quorum.
    {"test14", {1, 2, 3, 4, 5}, {{1, 101}, {2, 104}, {3, 103}, {4, 103}}, 103},
    // A similar case which is not final because another vote for >= 103 would change the outcome.
    {"test15", {1, 2, 3, 4, 5}, {{1, 101}, {2, 102}, {3, 103}, {4, 103}}, 102}
  };

  auto overlay = [](const craft::MajorityConfig& c,
                    const craft::AckedIndexer& l, uint64_t id,
                    craft::Index idx) {
    std::map<uint64_t, craft::Index> m;
    for (auto iid : c.IDs()) {
      if (iid == id) {
        m[iid] = idx;
      } else {
        auto [iidx, ok] = l.AckedIndex(iid);
        if (ok) {
          m[iid] = iidx;
        }
      }
    }
    return craft::MapAckIndexer(std::move(m));
  };

  for (auto& test : tests) {
    craft::MajorityConfig c(test.cfg);
    craft::MapAckIndexer l(test.idx);
    auto idx = c.CommittedIndex(l);
    ASSERT_EQ(idx, test.windex);
    // These alternative computations should return the same result.
    auto aidx = alternativeMajorityCommittedIndex(c, l);
    ASSERT_EQ(aidx, idx);
    // Joining a majority with the empty majority should give same result.
    aidx = craft::JointConfig(test.cfg, {}).CommittedIndex(l);
    ASSERT_EQ(aidx, idx);
    // Joining a majority with itself should give same result.
    aidx = craft::JointConfig(test.cfg, test.cfg).CommittedIndex(l);
    ASSERT_EQ(aidx, idx);
    for (auto id : c.IDs()) {
      auto [iidx, ok] = l.AckedIndex(id);
      if (idx > iidx && iidx > 0) {
        // If the committed index was definitely above the currently
        // inspected idx, the result shouldn't change if we lower it
        // further.
        auto lo = overlay(c, l, id, iidx-1);
        auto aidx = c.CommittedIndex(lo);
        ASSERT_EQ(aidx, idx);
        lo = overlay(c, l, id, 0);
        aidx = c.CommittedIndex(lo);
        ASSERT_EQ(aidx, idx);
      }
    }
  }
}

TEST(MajorityConfig, VoteResult) {
  struct Test {
    std::string name;
    std::set<uint64_t> cfg;
    std::map<uint64_t, bool> votes;
    craft::VoteState wstate;
  };

  std::vector<Test> tests = {
    // The empty config always announces a won vote.
    {"test0", {}, {}, craft::kVoteWon},
    {"test1", {1}, {}, craft::kVotePending},
    {"test2", {1}, {{1, false}}, craft::kVoteLost},
    {"test3", {123}, {{123, true}}, craft::kVoteWon},
    {"test4", {4, 8}, {}, craft::kVotePending},
    // With two voters, a single rejection loses the vote.
    {"test5", {4, 8}, {{4, false}}, craft::kVoteLost},
    {"test6", {4, 8}, {{4, true}}, craft::kVotePending},
    {"test7", {4, 8}, {{4, false}, {8, true}}, craft::kVoteLost},
    {"test8", {4, 8}, {{4, true}, {8, true}}, craft::kVoteWon},
    {"test9", {2, 4, 7}, {}, craft::kVotePending},
    {"test10", {2, 4, 7}, {{2, false}}, craft::kVotePending},
    {"test11", {2, 4, 7}, {{2, true}}, craft::kVotePending},
    {"test12", {2, 4, 7}, {{2, false}, {4, false}}, craft::kVoteLost},
    {"test13", {2, 4, 7}, {{2, true}, {4, false}}, craft::kVotePending},
    {"test14", {2, 4, 7}, {{2, true}, {4, true}}, craft::kVoteWon},
    {"test15", {2, 4, 7}, {{2, true}, {4, true}, {7, false}}, craft::kVoteWon},
    {"test16", {2, 4, 7}, {{2, false}, {4, true}, {7, false}}, craft::kVoteLost},
    // Test some random example with seven nodes (why not).
    {"test17", {1, 2, 3, 4, 5, 6, 7},
      {{1, true}, {2, true}, {3, false}, {4, true}}, craft::kVotePending},
    {"test18", {1, 2, 3, 4, 5, 6, 7},
      {{2, true}, {3, true}, {5, false}, {6, true}, {7, false}}, craft::kVotePending},
    {"test19", {1, 2, 3, 4, 5, 6, 7},
      {{1, true}, {2, true}, {3, false}, {4, true}, {6, false}, {7, true}}, craft::kVoteWon},
    {"test20", {1, 2, 3, 4, 5, 6, 7},
      {{1, true}, {2, true}, {4, false}, {5, true}, {6, false}, {7, false}}, craft::kVotePending},
    {"test21", {1, 2, 3, 4, 5, 6, 7},
      {{1, true}, {2, true}, {3, false}, {4, true}, {5, false}, {6, false}, {7, false}}, craft::kVoteLost}
  };

  for (Test& test : tests) {
    craft::MajorityConfig c(test.cfg);
    craft::VoteState gstate = c.VoteResult(test.votes);
    EXPECT_EQ(gstate, test.wstate);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}