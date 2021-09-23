#include "gtest/gtest.h"

#include <string>
#include <vector>

#define private public
#define protected public
#include "tracker/progress.h"
#undef private
#undef protected

class ProgressTest : public ::testing::Test {
protected:
    void SetUp() override {

    }

    // void TearDown() override {}

// protected:

};

TEST_F(ProgressTest, IsPaused) {
    struct Test {
        std::string name_;
        craft::StateType state_;
        bool probe_sent_;

        bool w_;
    };

    std::vector<Test> tests = {
        {"test0", craft::kStateProbe, false, false},
        {"test1", craft::kStateProbe, true, true},
        {"test2", craft::kStateReplicate, false, false},
        {"test3", craft::kStateReplicate, true, false},
        {"test4", craft::kStateSnapshot, false, true},
        {"test5", craft::kStateSnapshot, true, true}
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.state_ = test.state_;
        p.probe_sent_ = test.probe_sent_;
        p.inflights_.reset(new craft::Inflights(256));

        bool g = p.IsPaused();
        EXPECT_EQ(g, test.w_) << "#test case: " << test.name_;
    }
}

TEST_F(ProgressTest, Resume) {
    craft::Progress p;
    p.next_ = 2;
    p.probe_sent_ = true;

    p.MaybeDecrTo(1, 1);
    EXPECT_EQ(p.probe_sent_, false);

    p.probe_sent_ = true;
    p.MaybeUpdate(2);
    EXPECT_EQ(p.probe_sent_, false);
}

TEST_F(ProgressTest, BecomeProbe) {
    struct Test {
        std::string name_;
        craft::StateType state_;
        uint64_t match_;
        uint64_t next_;
        uint64_t pending_snapshot_;
        int32_t inflights_size_;

        uint64_t wnext_;
    };

    std::vector<Test> tests = {
        {"test0", craft::kStateReplicate, 1, 5, 0, 256, 2},
        {"test1", craft::kStateSnapshot, 1, 5, 10, 256, 11},
        {"test2", craft::kStateSnapshot, 1, 5, 0, 256, 2},
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.state_ = test.state_;
        p.match_ = test.match_;
        p.next_ = test.next_;
        p.pending_snapshot_ = test.pending_snapshot_;
        p.inflights_.reset(new craft::Inflights(test.inflights_size_));

        p.BecomeProbe();

        EXPECT_EQ(p.state_, craft::kStateProbe) << "#test case: " << test.name_;
        EXPECT_EQ(p.next_, test.wnext_) << "#test case: " << test.name_;
    }
}

TEST_F(ProgressTest, BecomeReplicate) {
    struct Test {
        std::string name_;
        craft::StateType state_;
        uint64_t match_;
        uint64_t next_;
        int32_t inflights_size_;
    };

    std::vector<Test> tests = {
        {"test0", craft::kStateProbe, 1, 5, 256}
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.state_ = test.state_;
        p.match_ = test.match_;
        p.next_ = test.next_;
        p.inflights_.reset(new craft::Inflights(test.inflights_size_));

        p.BecomeReplicate();

        EXPECT_EQ(p.state_, craft::kStateReplicate) << "#test case: " << test.name_;
        EXPECT_EQ(p.next_, test.match_+1) << "#test case: " << test.name_;
    }
}

TEST_F(ProgressTest, BecomeSnapshot) {
    struct Test {
        std::string name_;
        craft::StateType state_;
        uint64_t match_;
        uint64_t next_;
        int32_t inflights_size_;
        uint64_t snapshoti_;
    };

    std::vector<Test> tests = {
        {"test0", craft::kStateProbe, 1, 5, 256, 10}
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.state_ = test.state_;
        p.match_ = test.match_;
        p.next_ = test.next_;
        p.inflights_.reset(new craft::Inflights(test.inflights_size_));

        p.BecomeSnapshot(test.snapshoti_);

        EXPECT_EQ(p.state_, craft::kStateSnapshot) << "#test case: " << test.name_;
        EXPECT_EQ(p.pending_snapshot_, test.snapshoti_) << "#test case: " << test.name_;
    }
}

TEST_F(ProgressTest, MaybeUpdate) {
    struct Test {
        std::string name_;
        uint64_t update_;

        uint64_t wm_;
        uint64_t wn_;
        uint64_t wok_;
    };

    uint64_t prev_m = 3;
    uint64_t prev_n = 5;
    std::vector<Test> tests = {
        // do not decrease match, next
        {"test0", prev_m-1, prev_m, prev_n, false},
        // do not decrease next
        {"test1", prev_m, prev_m, prev_n, false},
        // increase match, do not decrease next
        {"test2", prev_m+1, prev_m+1, prev_n, true},
         // increase match, next
         {"test3", prev_m+2, prev_m+2, prev_n+1, true}
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.match_ = prev_m;
        p.next_ = prev_n;

        bool ok = p.MaybeUpdate(test.update_);
        EXPECT_EQ(ok, test.wok_) << "#test case: " << test.name_;
        EXPECT_EQ(p.match_, test.wm_) << "#test case: " << test.name_;
        EXPECT_EQ(p.next_, test.wn_) << "#test case: " << test.name_;
    }
}

TEST_F(ProgressTest, MaybeDecrTo) {
    struct Test {
        std::string name_;
        craft::StateType state_;
        uint64_t m_;
        uint64_t n_;
        uint64_t rejected_;
        uint64_t last_;

        bool w_;
        uint64_t wn_;
    };

    std::vector<Test> tests = {
        // state replicate and rejected is not greater than match
        {"test0", craft::kStateReplicate, 5, 10, 5, 5, false, 10},
        // state replicate and rejected is not greater than match
        {"test1", craft::kStateReplicate, 5, 10, 4, 4, false, 10},
        // state replicate and rejected is greater than match
        // directly decrease to match+1
        {"test2", craft::kStateReplicate, 5, 10, 9, 9, true, 6},
        // next-1 != rejected is always false
        {"test3", craft::kStateProbe, 0, 0, 0, 0, false, 0},
        // next-1 != rejected is always false
        {"test4", craft::kStateProbe, 0, 10, 5, 5, false, 10},
        // next>1 = decremented by 1
        {"test5", craft::kStateProbe, 0, 10, 9, 9, true, 9},
        // next>1 = decremented by 1
        {"test6", craft::kStateProbe, 0, 2, 1, 1, true, 1},
        // next<=1 = reset to 1
        {"test7", craft::kStateProbe, 0, 1, 0, 0, true, 1},
        // // decrease to min(rejected, last+1)
        {"test8", craft::kStateProbe, 0, 10, 9, 2, true, 3},
        // rejected < 1, reset to 1
        {"test9", craft::kStateProbe, 0, 10, 9, 0, true, 1}
    };

    for (Test& test : tests) {
        craft::Progress p;
        p.state_ = test.state_;
        p.match_ = test.m_;
        p.next_ = test.n_;
        
        bool g = p.MaybeDecrTo(test.rejected_, test.last_);
        EXPECT_EQ(g, test.w_) << "#test case: " << test.name_;
        EXPECT_EQ(p.match_, test.m_) << "#test case: " << test.name_;
        EXPECT_EQ(p.next_, test.wn_) << "#test case: " << test.name_;
    }
}