#include "gtest/gtest.h"

#include <vector>

#define private public
#define protected public
#include "craft/log_unstable.h"
#undef private
#undef protected

static raftpb::Entry make_entry(uint64_t index, uint64_t term);
static raftpb::Snapshot* make_snapshot(uint64_t index, uint64_t term);

class UnstableTest : public ::testing::Test {
protected:
    // void SetUp() override {}
    // void TearDown() override {}

};

static raftpb::Entry make_entry(uint64_t index, uint64_t term) {
    raftpb::Entry ent;
    
    ent.set_index(index);
    ent.set_term(term);

    return ent;
}

static raftpb::Snapshot* make_snapshot(uint64_t index, uint64_t term) {
    raftpb::Snapshot* snapshot = new raftpb::Snapshot;
    snapshot->mutable_metadata()->set_index(index);
    snapshot->mutable_metadata()->set_term(term);

    return snapshot;
};

TEST_F(UnstableTest, MaybeFirstIndex) {
    struct Test
    {
        std::vector<raftpb::Entry> entries_;
        uint64_t offset_;
        raftpb::Snapshot* snap_;
        
        uint64_t windex_;
    };

    std::vector<Test> tests = {
        // no snapshot
        {{make_entry(5, 1)}, 5, nullptr, 0},
        {{}, 0, nullptr, 0},
        // has snapshot
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 5},
        {{}, 5, make_snapshot(4, 1), 5} 
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        craft::Unstable u;
        u.entries_ = test.entries_;
        u.offset_ = test.offset_;
        u.snapshot_.reset(test.snap_);

        uint64_t index = u.MaybeFirstIndex();
        EXPECT_EQ(index, test.windex_) << "#test case: " << i;
    }
}

TEST_F(UnstableTest, LastIndex) {
    struct Test {
        std::vector<raftpb::Entry> entries_;
        uint64_t offset_;
        raftpb::Snapshot* snap_;

        uint64_t windex_;
    };

    std::vector<Test> tests = {
        // last in entries
        {{make_entry(5, 1)}, 5, nullptr, 5},
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 5},
        // last in snapshot
        {{}, 5, make_snapshot(4, 1), 4},
        // empty unstable
        {{}, 0, nullptr, 0}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        craft::Unstable u;
        u.entries_ = test.entries_;
        u.offset_ = test.offset_;
        u.snapshot_.reset(test.snap_);

        uint64_t index = u.MaybeLastIndex();
        EXPECT_EQ(index, test.windex_) << "#test case: " << i;
    }
}

TEST_F(UnstableTest, MaybeTerm) {
    struct Test
    {
        std::vector<raftpb::Entry> entries_;
        uint64_t offset_;
        raftpb::Snapshot* snap_;
        uint64_t index_;

        uint64_t wterm_;
    };

    std::vector<Test> tests = {
        // term from entries
        {{make_entry(4, 1)}, 5, nullptr, 5, 1},
        {{make_entry(5, 1)}, 5, nullptr, 6, 0},
        {{make_entry(5, 1)}, 5, nullptr, 4, 0},
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 5, 1},
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 6, 0},
        // term from snapshot
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 4, 1},
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 3, 0},
        {{}, 5, make_snapshot(4, 1), 5, 0},
        {{}, 5, make_snapshot(4, 1), 4, 1},
        {{}, 0, nullptr, 5, 0}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        craft::Unstable u;
        u.entries_ = test.entries_;
        u.offset_ = test.offset_;
        u.snapshot_.reset(test.snap_);

        uint64_t term = u.MaybeTerm(test.index_);
        EXPECT_EQ(term, test.wterm_) << "#test case: " << i;
    }
}

TEST_F(UnstableTest, Restore) {
    craft::Unstable u;
    u.entries_ = {make_entry(4, 1)};
    u.offset_ = 5;
    u.snapshot_.reset(make_snapshot(4, 1));

    std::unique_ptr<raftpb::Snapshot> s(make_snapshot(6, 2));
    u.Restore(*s);

    EXPECT_EQ(u.offset_, (s->metadata().index()+1));
    EXPECT_TRUE(u.entries_.empty());
    ASSERT_TRUE(u.snapshot_ != nullptr);
    EXPECT_EQ(u.snapshot_->metadata().index(), s->metadata().index());
    EXPECT_EQ(u.snapshot_->metadata().term(), s->metadata().term());
}

TEST_F(UnstableTest, StableTo) {
    struct Test
    {
        std::vector<raftpb::Entry> entries_;
        uint64_t offset_;
        raftpb::Snapshot* snap_;
        uint64_t index_;
        uint64_t term_;

        uint64_t woffset_;
        uint64_t wlen_;
    };

    std::vector<Test> tests = {
        {{}, 0, nullptr, 5, 1, 0, 0},
        // stable to the first entry
        {{make_entry(5, 1)}, 5, nullptr, 5, 1, 6, 0},
        // stable to the first entry
        {{make_entry(5, 1), make_entry(6, 1)}, 5, nullptr, 5, 1, 6, 1},
        // stable to the first entry and term mismatch
        {{make_entry(6, 2)}, 6, nullptr, 6, 1, 6, 1},
        // stable to old entry
        {{make_entry(5, 1)}, 5, nullptr, 4, 1, 5, 1},
        // with snapshot
        // stable to the first entry
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 5, 1, 6, 0},
        // stable to the first entry
        {{make_entry(5, 1), make_entry(6, 1)}, 5, make_snapshot(4, 1), 5, 1, 6, 1},
        // stable to the first entry and term mismatch
        {{make_entry(6, 2)}, 6, make_snapshot(5, 1), 6, 1, 6, 1},
        // stable to snapshot
        {{make_entry(5, 1)}, 5, make_snapshot(4, 1), 4, 1, 5, 1},
        // stable to old entry
        {{make_entry(5, 2)}, 5, make_snapshot(4, 2), 4, 1, 5, 1}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        craft::Unstable u;
        u.entries_ = test.entries_;
        u.offset_ = test.offset_;
        u.snapshot_.reset(test.snap_);

        u.StableTo(test.index_, test.term_);
        EXPECT_EQ(u.offset_, test.woffset_) << "#test case: " << i;
        EXPECT_EQ(static_cast<uint64_t>(u.entries_.size()), test.wlen_) << "#test case: " << i;
    }
}

TEST_F(UnstableTest, TruncateAndAppend) {
    struct Test
    {
        std::vector<raftpb::Entry> entries_;
        uint64_t offset_;
        raftpb::Snapshot* snap_;
        std::vector<raftpb::Entry> toappend_;
    
        uint64_t woffset_;
        std::vector<raftpb::Entry> wentries_;
    };

    std::vector<Test> tests = {
        // append to the end
        {
            {make_entry(5, 1)}, 5, nullptr,
            {make_entry(6, 1), make_entry(7, 1)},
            5, {make_entry(5, 1), make_entry(6, 1), make_entry(7, 1)}
        },
        // replace the unstable entries
        {
            {make_entry(5, 1)}, 5, nullptr,
            {make_entry(5, 2), make_entry(6, 2)},
            5, {make_entry(5, 2), make_entry(6, 2)}
        },
        {
            {make_entry(5, 1)}, 5, nullptr,
            {make_entry(4, 2), make_entry(5, 2), make_entry(6, 2)},
            4, {make_entry(4, 2), make_entry(5, 2), make_entry(6, 2)}
        },
        // truncate the existing entries and append
        {
            {make_entry(5, 1), make_entry(6, 1), make_entry(7, 1)}, 5, nullptr,
            {make_entry(6, 2)},
            5, {make_entry(5, 1), make_entry(6, 2)}
        },
        {
            {make_entry(5, 1), make_entry(6, 1), make_entry(7, 1)}, 5, nullptr,
            {make_entry(7, 2), make_entry(8, 2)},
            5, {make_entry(5, 1), make_entry(6, 1), make_entry(7, 2), make_entry(8, 2)}
        }
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        craft::Unstable u;
        u.entries_ = test.entries_;
        u.offset_ = test.offset_;
        u.snapshot_.reset(test.snap_);

        u.TruncateAndAppend(test.toappend_);

        EXPECT_EQ(u.offset_, test.woffset_) << "#test case: " << i;
        EXPECT_EQ(u.entries_.size(), test.wentries_.size()) << "#test case: " << i;
        if (u.entries_.size() != test.wentries_.size()) {
            continue;
        }

        for (int i = 0; i < u.entries_.size(); i++) {
            EXPECT_EQ(u.entries_[i].index(), test.wentries_[i].index()) << "#test case: " << i;
            EXPECT_EQ(u.entries_[i].term(), test.wentries_[i].term()) << "#test case: " << i;
        }
    }
}

int _tmain(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}