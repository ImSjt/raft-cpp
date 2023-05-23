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
#include <vector>
#include <set>
#include <map>
#include <memory>

#include "gtest/gtest.h"
#include "src/quorum/joint.h"

TEST(JointConfig, CommittedIndex) {
  struct Test {
    std::string name;
    std::set<uint64_t> cfg;
    std::set<uint64_t> cfgj;
    std::map<uint64_t, uint64_t> idx;
    uint64_t windex;
  };

  std::vector<Test> tests = {
    {"test0", {1, 2, 3}, {}, {{1, 100}, {2, 101}, {3, 99}}, 100},
    // Joint nonoverlapping singleton quorums.
    {"test1", {1}, {2}, {}, 0},
    // Voter 1 has 100 committed, 2 nothing. This means we definitely won't commit
    // past 100.
    {"test2", {1}, {2}, {{1, 100}}, 0},
    // Committed index collapses once both majorities do, to the lower index.
    {"test3", {1}, {2}, {{1, 13}, {2, 100}}, 13},
    // Joint overlapping (i.e. identical) singleton quorum.
    {"test4", {1}, {1}, {}, 0},
    {"test5", {1}, {1}, {{1, 100}}, 100},
    // Two-node config joint with non-overlapping single node config
    {"test6", {1, 3}, {2}, {}, 0},
    {"test7", {1, 3}, {2}, {{1, 100}}, 0},
    {"test8", {1, 3}, {2}, {{1, 100}, {2, 50}}, 0},
    // 2 reports 45, collapsing the other half (to 45).
    {"test9", {1, 3}, {2}, {{1, 100}, {2, 45}, {3, 50}}, 45},
    // Two-node config with overlapping single-node config.
    {"test10", {1, 2}, {2}, {}, 0},
    // 1 reports 100.
    {"test11", {1, 2}, {2}, {{1, 100}}, 0},
    // 2 reports 100.
    {"test12", {1, 2}, {2}, {{2, 100}}, 0},
    {"test13", {1, 2}, {2}, {{1, 50}, {2, 100}}, 50},
    {"test14", {1, 2}, {2}, {{1, 100}, {2, 50}}, 50},
    // Joint non-overlapping two-node configs.
    {"test15", {1, 2}, {3, 4}, {{1, 50}}, 0},
    {"test16", {1, 2}, {3, 4}, {{1, 50}, {3, 49}}, 0},
    {"test17", {1, 2}, {3, 4}, {{1, 50}, {2, 48}, {3, 49}}, 0},
    {"test18", {1, 2}, {3, 4}, {{1, 50}, {2, 48}, {3, 49}, {4, 47}}, 47},
    // Joint overlapping two-node configs.
    {"test19", {1, 2}, {2, 3}, {}, 0},
    {"test20", {1, 2}, {2, 3}, {{1, 100}}, 0},
    {"test21", {1, 2}, {2, 3}, {{2, 100}}, 0},
    {"test22", {1, 2}, {2, 3}, {{2, 100}, {3, 99}}, 0},
    {"test23", {1, 2}, {2, 3}, {{1, 101}, {2, 100}, {3, 99}}, 99},
    // Joint identical two-node configs.
    {"test24", {1, 2}, {1, 2}, {}, 0},
    {"test25", {1, 2}, {1, 2}, {{2, 40}}, 0},
    {"test26", {1, 2}, {1, 2}, {{1, 41}, {2, 40}}, 40},
    // Joint disjoint three-node configs.
    {"test27", {1, 2, 3}, {4, 5, 6}, {}, 0},
    {"test28", {1, 2, 3}, {4, 5, 6}, {{1, 100}}, 0},
    {"test29", {1, 2, 3}, {4, 5, 6}, {{1, 100}, {2, 99}}, 0},
    // First quorum <= 99, second one <= 97. Both quorums guarantee that 90 is
    // committed.
    {"test30", {1, 2, 3}, {4, 5, 6}, {{2, 99}, {3, 90}, {4, 97}, {5, 95}}, 90},
    // First quorum collapsed to 92. Second one already had at least 95 committed,
    // so the result also collapses.
    {"test31", {1, 2, 3}, {4, 5, 6}, {{1, 92}, {2, 99}, {3, 90}, {4, 97}, {5, 95}}, 92},
    {"test32", {1, 2, 3}, {4, 5, 5}, {{1, 92}, {2, 99}, {3, 90}, {4, 97}, {5, 95}, {6, 77}}, 92},
    // Joint overlapping three-node configs.
    {"test33", {1, 2, 3}, {1, 4, 5}, {}, 0},
    {"test34", {1, 2, 3}, {1, 4, 5}, {{1, 100}}, 0},
    {"test35", {1, 2, 3}, {1, 4, 5}, {{1, 100}, {2, 101}}, 0},
    {"test36", {1, 2, 3}, {1, 4, 5}, {{1, 100}, {2, 101}, {3, 100}}, 0},
    // Second quorum could commit either 98 or 99, but first quorum is open.
    {"test37", {1, 2, 3}, {1, 4, 5}, {{2, 100}, {4, 99}, {5, 98}}, 0},
    // Additionally, first quorum can commit either 100 or 99
    {"test38", {1, 2, 3}, {1, 4, 5}, {{2, 100}, {3, 99}, {4, 99}, {5, 98}}, 98},
    {"test39", {1, 2, 3}, {1, 4, 5}, {{1, 1}, {2, 100}, {3, 99}, {4, 99}, {5, 98}}, 98},
    {"test40", {1, 2, 3}, {1, 4, 5}, {{1, 100}, {2, 100}, {3, 99}, {4, 99}, {5, 98}}, 99},
    // More overlap.
    {"test41", {1, 2, 3}, {2, 3, 4}, {}, 0},
    {"test42", {1, 2, 3}, {2, 3, 4}, {{2, 100}, {3, 99}}, 99},
    {"test43", {1, 2, 3}, {2, 3, 4}, {{2, 100}, {3, 99}}, 99},
    {"test44", {1, 2, 3}, {2, 3, 4}, {{1, 98}, {2, 100}, {3, 99}}, 99},
    {"test45", {1, 2, 3}, {2, 3, 4}, {{1, 100}, {2, 100}, {3, 99}}, 99},
    {"test46", {1, 2, 3}, {2, 3, 4}, {{1, 100}, {2, 100}, {3, 99}, {4, 98}}, 99},
    {"test47", {1, 2, 3}, {2, 3, 4}, {{1, 100}, {4, 101}}, 0},
    {"test48", {1, 2, 3}, {2, 3, 4}, {{1, 100}, {2, 99}, {4, 101}}, 99},
    // Identical. This is also exercised in the test harness, so it's listed here
    // only briefly.
    {"test49", {1, 2, 3}, {1, 2, 3}, {{1, 50}, {2, 45}}, 45}
  };

  for (Test& test : tests) {
    craft::JointConfig joint_config(test.cfg, test.cfgj);
    craft::MapAckIndexer indexer(test.idx);
    uint64_t idx = joint_config.CommittedIndex(indexer);
    ASSERT_EQ(idx, test.windex);

    auto aidx = craft::JointConfig(test.cfgj, test.cfg).CommittedIndex(indexer);
    ASSERT_EQ(aidx, idx);
  }
}

TEST(JointConfig, VoteResult) {
  struct Test {
    std::string name;
    std::set<uint64_t> cfg;
    std::set<uint64_t> cfgj;
    std::map<uint64_t, bool> votes;
    uint64_t wstate;
  };

  std::vector<Test> tests = {
    {"test0", {}, {}, {}, craft::kVoteWon},
    // More examples with close to trivial configs.
    {"test1", {1}, {}, {}, craft::kVotePending},
    {"test2", {1}, {}, {{1, true}}, craft::kVoteWon},
    {"test3", {1}, {}, {{1, false}}, craft::kVoteLost},
    {"test4", {1}, {1}, {}, craft::kVotePending},
    {"test5", {1}, {1}, {{1, true}}, craft::kVoteWon},
    {"test6", {1}, {1}, {{1, false}}, craft::kVoteLost},
    {"test7", {1}, {2}, {}, craft::kVotePending},
    {"test8", {1}, {2}, {{1, true}}, craft::kVotePending},
    {"test9", {1}, {2}, {{1, true}, {2, true}}, craft::kVoteWon},
    {"test10", {1}, {2}, {{1, true}, {2, true}}, craft::kVoteWon},
    {"test11", {1}, {2}, {{1,true}, {2, false}}, craft::kVoteLost},
    {"test12", {1}, {2}, {{1, false}}, craft::kVoteLost},
    {"test13", {1}, {2}, {{1, false}, {2, false}}, craft::kVoteLost},
    {"test14", {1}, {2}, {{1, false}, {2, true}}, craft::kVoteLost},
    // Two node configs.
    {"test15", {1, 2}, {3, 4}, {}, craft::kVotePending},
    {"test16", {1, 2}, {3, 4}, {{1, true}}, craft::kVotePending},
    {"test17", {1, 2}, {3, 4}, {{1, true}, {2, true}}, craft::kVotePending},
    {"test18", {1, 2}, {3, 4}, {{1, true}, {2, true}, {3, false}}, craft::kVoteLost},
    {"test19", {1, 2}, {3, 4}, {{1, true}, {2, true}, {3, false}, {4, false}}, craft::kVoteLost},
    {"test20", {1, 2}, {3, 4}, {{1, true}, {2, true}, {3, true}, {4, false}}, craft::kVoteLost},
    {"test21", {1, 2}, {3, 4}, {{1, true}, {2, true}, {3, true}, {4, true}}, craft::kVoteWon},
    {"test22", {1, 2}, {2, 3}, {}, craft::kVotePending},
    {"test23", {1, 2}, {2, 3}, {{2, false}}, craft::kVoteLost},
    {"test24", {1, 2}, {2, 3}, {{1, true}, {2, true}}, craft::kVotePending},
    {"test25", {1, 2}, {2, 3}, {{1, true}, {2, true}, {3, false}}, craft::kVoteLost},
    {"test26", {1, 2}, {2, 3}, {{1, true}, {2, true}, {3, true}}, craft::kVoteWon},
    {"test27", {1, 2}, {1, 2}, {}, craft::kVotePending},
    {"test28", {1, 2}, {1, 2}, {{1, true}}, craft::kVotePending},
    {"test29", {1, 2}, {1, 2}, {{1, true}, {2, false}}, craft::kVoteLost},
    {"test30", {1, 2}, {1, 2}, {{1, false}}, craft::kVoteLost},
    {"test31", {1, 2}, {1, 2}, {{1, false}, {2, false}}, craft::kVoteLost},
    // Simple example for overlapping three node configs.
    {"test32", {1, 2, 3}, {2, 3, 4}, {}, craft::kVotePending},
    {"test33", {1, 2, 3}, {2, 3, 4}, {{2, false}}, craft::kVotePending},
    {"test34", {1, 2, 3}, {2, 3, 4}, {{2, false}, {3, false}}, craft::kVoteLost},
    {"test35", {1, 2, 3}, {2, 3, 4}, {{2, true}, {3, true}}, craft::kVoteWon},
    {"test36", {1, 2, 3}, {2, 3, 4}, {{1, true}, {2, true}}, craft::kVotePending},
    {"test37", {1, 2, 3}, {2, 3, 4}, {{1, true}, {2, true}, {3, false}}, craft::kVotePending},
    {"test38", {1, 2, 3}, {2, 3, 4}, {{1, true}, {2, true}, {3, false}, {4, false}}, craft::kVoteLost},
    {"test39", {1, 2, 3}, {2, 3, 4}, {{1, true}, {2, true}, {3, false}, {4, true}}, craft::kVoteWon},
  };

  for (Test& test : tests) {
    craft::JointConfig joint_config(test.cfg, test.cfgj);
    uint64_t gstate = joint_config.VoteResult(test.votes);
    ASSERT_EQ(gstate, test.wstate);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}