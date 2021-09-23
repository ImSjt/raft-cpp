#include "gtest/gtest.h"

// #include <cstring>
// #include <cassert>
#include <vector>
#include <set>
#include <map>
#include <memory>

#define private public
#define protected public
#include "quorum/majority.h"
#undef private
#undef protected

class MajorityConfigTest : public ::testing::Test {
protected:
    void SetUp() override {

    }

    // void TearDown() override {}

// protected:

};

TEST_F(MajorityConfigTest, CommittedIndex) {
    struct Test {
        std::string name_;
        std::set<uint64_t> cfg_;
        std::map<uint64_t, uint64_t> idx_;
        uint64_t windex_;
    };

    std::vector<Test> tests = {
        // The empty quorum commits "everything". This is useful for its use in joint
        // quorums.
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
        // A similar case which is not final because another vote for >= 103 would change
        // the outcome.
        {"test15", {1, 2, 3, 4, 5}, {{1, 101}, {2, 102}, {3, 103}, {4, 103}}, 102}
    };
    

    for (Test& test : tests) {
        craft::MajorityConfig majority_config(test.cfg_);
        craft::MapAckIndexer indexer(test.idx_);

        uint64_t gindex = majority_config.CommittedIndex(indexer);
        EXPECT_EQ(gindex, test.windex_) << "#test case: " << test.name_;
    }
}

TEST_F(MajorityConfigTest, VoteResult) {
    struct Test {
        std::string name_;
        std::set<uint64_t> cfg_;
        std::map<uint64_t, bool> votes_;
        craft::VoteState wstate_;
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
        {"test17", {1, 2, 3, 4, 5, 6, 7}, {{1, true}, {2, true}, {3, false}, {4, true}}, craft::kVotePending},
        {"test18", {1, 2, 3, 4, 5, 6, 7}, {{2, true}, {3, true}, {5, false}, {6, true}, {7, false}}, craft::kVotePending},
        {"test19", {1, 2, 3, 4, 5, 6, 7}, {{1, true}, {2, true}, {3, false}, {4, true}, {6, false}, {7, true}}, craft::kVoteWon},
        {"test20", {1, 2, 3, 4, 5, 6, 7}, {{1, true}, {2, true}, {4, false}, {5, true}, {6, false}, {7, false}}, craft::kVotePending},
        {"test21", {1, 2, 3, 4, 5, 6, 7}, {{1, true}, {2, true}, {3, false}, {4, true}, {5, false}, {6, false}, {7, false}}, craft::kVoteLost}
    };

    for (Test& test : tests) {
        craft::MajorityConfig majority_config(test.cfg_);

        craft::VoteState gstate = majority_config.VoteResult(test.votes_);
        EXPECT_EQ(gstate, test.wstate_) << "#test case: " << test.name_;
    }
}