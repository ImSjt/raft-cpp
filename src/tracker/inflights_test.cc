#include "gtest/gtest.h"

#include <vector>
#include <set>
#include <map>
#include <memory>

#define private public
#define protected public
#include "tracker/inflights.h"
#undef private
#undef protected

class InflightsTest : public ::testing::Test {
protected:
    void SetUp() override {

    }

    // void TearDown() override {}

// protected:

};

static craft::Inflights make_inflights(int32_t start, int32_t count, int32_t size, std::vector<uint64_t> buffer) {
    craft::Inflights in(size);
    
    in.start_ = start;
    in.count_ = count;
    in.buffer_ = buffer;

    return in;
}

TEST_F(InflightsTest, Add) {
    struct Test {
        std::string name_;
        craft::Inflights in_;
        std::vector<uint64_t> add_;
        craft::Inflights win_;
    };

    std::vector<Test> tests = {
        {"test0", make_inflights(0, 0, 10, std::vector<uint64_t>(10)), {0, 1, 2, 3, 4}, make_inflights(0, 5, 10, {0, 1, 2, 3, 4, 0, 0, 0, 0, 0})},
        {"test1", make_inflights(0, 0, 10, std::vector<uint64_t>(10)), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, make_inflights(0, 10, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})},
        {"test2", make_inflights(5, 0, 10, std::vector<uint64_t>(10)), {0, 1, 2, 3, 4}, make_inflights(5, 5, 10, {0, 0, 0, 0, 0, 0, 1, 2, 3, 4})},
        {"test3", make_inflights(5, 0, 10, std::vector<uint64_t>(10)), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, make_inflights(5, 10, 10, {5, 6, 7, 8, 9, 0, 1, 2, 3, 4})},
    };

    for (Test& test : tests) {
        for (uint64_t i : test.add_) {
            test.in_.Add(i);
        }

        EXPECT_EQ(test.in_.start_, test.win_.start_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.count_, test.win_.count_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.size_, test.win_.size_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.buffer_, test.win_.buffer_) << "#test case: " << test.name_;
    }
}

TEST_F(InflightsTest, FreeLE) {
    struct Test {
        std::string name_;
        craft::Inflights in_;
        uint64_t free_to_;
        craft::Inflights win_;
    };

    std::vector<Test> tests = {
        {"test0", make_inflights(0, 10, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), 4, make_inflights(5, 5, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})},
        {"test1", make_inflights(0, 10, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), 8, make_inflights(9, 1, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})},
        {"test2", make_inflights(9, 6, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9}), 12, make_inflights(3, 2, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9})},
        {"test3", make_inflights(3, 2, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9}), 14, make_inflights(0, 0, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9})},
    };

    for (Test& test : tests) {
        test.in_.FreeLE(test.free_to_);

        EXPECT_EQ(test.in_.start_, test.win_.start_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.count_, test.win_.count_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.size_, test.win_.size_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.buffer_, test.win_.buffer_) << "#test case: " << test.name_;
    }
}

TEST_F(InflightsTest, FreeFirstOne) {
    struct Test {
        std::string name_;
        craft::Inflights in_;
        craft::Inflights win_;
    };

    std::vector<Test> tests = {
        {"test0", make_inflights(0, 10, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), make_inflights(1, 9, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})}
    };

    for (Test& test : tests) {
        test.in_.FreeFirstOne();

        EXPECT_EQ(test.in_.start_, test.win_.start_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.count_, test.win_.count_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.size_, test.win_.size_) << "#test case: " << test.name_;
        EXPECT_EQ(test.in_.buffer_, test.win_.buffer_) << "#test case: " << test.name_;
    }
}