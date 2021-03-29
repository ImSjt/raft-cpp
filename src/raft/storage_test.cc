#include "gtest/gtest.h"

#define private public
#define protected public
#include "craft/storage.h"
#undef private
#undef protected

static raftpb::Entry make_entry(uint64_t index, uint64_t term);

class MemoryStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        for (uint64_t i = 3; i <= 5; i++) {
            ents_.push_back(make_entry(i, i));
        }

        storage_.ents_ = ents_; // [3, 4, 5]
    }

    // void TearDown() override {}

protected:
    std::vector<raftpb::Entry> ents_;
    craft::MemoryStorage storage_;

};

static raftpb::Entry make_entry(uint64_t index, uint64_t term) {
    raftpb::Entry ent;
    
    ent.set_index(index);
    ent.set_term(term);

    return ent;
}

TEST_F(MemoryStorageTest, Term) {
    struct Test {
        uint64_t index_;
        uint64_t wterm_;
        craft::Status wstatus_;
    };

    std::vector<Test> tests = {
        {2, 0, craft::Status::Error("requested index is unavailable due to compaction [offset: %d, i: 2]", storage_.ents_[0].index())},
        {3, 3, craft::Status::OK()},
        {4, 4, craft::Status::OK()},
        {5, 5, craft::Status::OK()},
        {6, 0, craft::Status::Error("requested entry at index is unavailable [rel_index: %d, len(ents_): %d]", 6-(storage_.ents_[0].index()), storage_.ents_.size())}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        uint64_t term;
        craft::Status status = storage_.Term(test.index_, term);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;
        EXPECT_EQ(term, test.wterm_) << "#test case: " << i;
    }
}

TEST_F(MemoryStorageTest, Entries) {
    raftpb::Entry entry = make_entry(6, 6);
    std::vector<raftpb::Entry> ents = {entry};
    storage_.Append(ents); // [3, 4, 5, 6]

    struct Test
    {
        uint64_t lo_;
        uint64_t hi_;
        uint64_t max_size_;
        craft::Status wstatus_;
        std::vector<raftpb::Entry> wentries_;
    };

    std::vector<Test> tests = {
        {2, 6, std::numeric_limits<uint64_t>::max(), craft::Status::Error("requested index is unavailable due to compaction [offset: %d, lo: 2]", storage_.ents_[0].index()), {}},
        {3, 4, std::numeric_limits<uint64_t>::max(), craft::Status::Error("requested index is unavailable due to compaction [offset: %d, lo: 3]", storage_.ents_[0].index()), {}},
        {4, 5, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {make_entry(4, 4)}},
        {4, 6, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {make_entry(4, 4), make_entry(5, 5)}},
        {4, 7, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {make_entry(4, 4), make_entry(5, 5), make_entry(6, 6)}},
        {4, 7, 0, craft::Status::OK(), {}},
        {4, 7, static_cast<uint64_t>(entry.ByteSizeLong())*2 + static_cast<uint64_t>(entry.ByteSizeLong())/2, craft::Status::OK(), std::vector<raftpb::Entry>{make_entry(4, 4), make_entry(5, 5)}},
        {4, 7, static_cast<uint64_t>(entry.ByteSizeLong())*3 - 1, craft::Status::OK(), {make_entry(4, 4), make_entry(5, 5)}},
        {4, 7, static_cast<uint64_t>(entry.ByteSizeLong())*3, craft::Status::OK(), {make_entry(4, 4), make_entry(5, 5), make_entry(6, 6)}}
    };
    
    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        std::vector<raftpb::Entry> entries;
        craft::Status status = storage_.Entries(test.lo_, test.hi_, test.max_size_, entries);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;
        EXPECT_EQ(entries.size(), test.wentries_.size()) << "#test case: " << i;

        if (entries.size() != test.wentries_.size()) {
            continue;
        }

        for (int i = 0; i < entries.size(); i++) {
            EXPECT_EQ(entries[i].index(), test.wentries_[i].index()) << "#test case: " << i;
            EXPECT_EQ(entries[i].term(), test.wentries_[i].term()) << "#test case: " << i;
        }
    }
}

TEST_F(MemoryStorageTest, LastIndex) {
    uint64_t last = storage_.LastIndex();
    EXPECT_EQ(last, 5);

    std::vector<raftpb::Entry> ents = {make_entry(6, 5)};
    storage_.Append(ents);
    last = storage_.LastIndex();
    EXPECT_EQ(last, 6);
}

TEST_F(MemoryStorageTest, FirstIndex) {
    uint64_t first = storage_.FirstIndex();
    EXPECT_EQ(first, 4);

    storage_.Compact(4);
    first = storage_.FirstIndex();
    EXPECT_EQ(first, 5);
}

TEST_F(MemoryStorageTest, Compact) {
    struct Test
    {
        uint64_t i_;
        craft::Status wstatus_;
        uint64_t windex_;
        uint64_t wterm_;
        uint64_t wlen_;
    };

    std::vector<Test> tests = {
        {2, craft::Status::Error("requested index is unavailable due to compaction [compact_index: 2, offset: %d]", storage_.ents_[0].index()), 3, 3, 3},
        {3, craft::Status::Error("requested index is unavailable due to compaction [compact_index: 3, offset: %d]", storage_.ents_[0].index()), 3, 3, 3},
        {4, craft::Status::OK(), 4, 4, 2},
        {5, craft::Status::OK(), 5, 5, 1}
    };
    
    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        craft::MemoryStorage storage;
        storage.ents_ = ents_; // [3, 4, 5]

        craft::Status status = storage.Compact(test.i_);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;

        uint64_t ent0_index = storage.ents_[0].index();
        EXPECT_EQ(ent0_index, test.windex_) << "#test case: " << i;

        uint64_t term;
        status = storage.Term(ent0_index, term);
        EXPECT_STREQ(status.Str(), "OK") << "#test case: " << i;
        EXPECT_EQ(term, test.wterm_) << "#test case: " << i;
        EXPECT_EQ(static_cast<uint64_t>((storage.ents_.size())), test.wlen_) << "#test case: " << i;
    }
}

TEST_F(MemoryStorageTest, CreateSnapshot) {
    raftpb::ConfState cs;
    cs.add_voters(1);
    cs.add_voters(2);
    cs.add_voters(3);

    std::string data("data");

    auto make_snapshot = [&cs, &data](uint64_t index, uint64_t term){
        raftpb::Snapshot snapshot;
        snapshot.set_data(data);
        snapshot.mutable_metadata()->set_index(index);
        snapshot.mutable_metadata()->set_term(term);
        *(snapshot.mutable_metadata()->mutable_conf_state()) = cs;

        return snapshot;
    };

    struct Test
    {
        uint64_t i_;
        craft::Status wstatus_;
        raftpb::Snapshot wsnap_;
    };

    std::vector<Test> tests = {
        {4, craft::Status::OK(), make_snapshot(4, 4)},
        {5, craft::Status::OK(), make_snapshot(5, 5)},
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        craft::MemoryStorage storage;
        storage.ents_ = ents_; // [3, 4, 5]

        raftpb::Snapshot snapshot;
        craft::Status status = storage.CreateSnapshot(test.i_, &cs, data, snapshot);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;
        if (!status.ok()) {
            continue;
        }

        EXPECT_EQ(snapshot.data(), test.wsnap_.data()) << "#test case: " << i;
        EXPECT_EQ(snapshot.metadata().index(), test.wsnap_.metadata().index()) << "#test case: " << i;
        EXPECT_EQ(snapshot.metadata().term(), test.wsnap_.metadata().term()) << "#test case: " << i;

        auto voters = snapshot.metadata().conf_state().voters();
        auto wvoters = test.wsnap_.metadata().conf_state().voters();
        EXPECT_EQ(voters.size(), wvoters.size()) << "#test case: " << i;
        if (voters.size() != wvoters.size()) {
            continue;
        }

        for (int i = 0; i < voters.size(); i++) {
            EXPECT_EQ(voters[i], wvoters[i]) << "#test case: " << i;
        }
    }
}

TEST_F(MemoryStorageTest, Append) {
    struct Test
    {
        std::vector<raftpb::Entry> entries_;
        craft::Status wstatus_;
        std::vector<raftpb::Entry> wentries_;
    };
    
    std::vector<Test> tests = {
        {
            {make_entry(1, 1), make_entry(2, 2)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5)}
        },
        {
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5)}
        },
        {
            {make_entry(3, 3), make_entry(4, 6), make_entry(5, 6)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 6), make_entry(5, 6)}
        },
        {
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5), make_entry(6, 5)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5), make_entry(6, 5)}
        },
        // truncate incoming entries, truncate the existing entries and append
        {
            {make_entry(2, 3), make_entry(3, 3), make_entry(4, 5)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 5)}
        },
        {
            {make_entry(4, 5)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 5)}
        },
        // direct append
        {
            {make_entry(6, 5)},
            craft::Status::OK(),
            {make_entry(3, 3), make_entry(4, 4), make_entry(5, 5), make_entry(6, 5)}
        }
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        craft::MemoryStorage storage;
        storage.ents_ = ents_; // [3, 4, 5]

        craft::Status status = storage.Append(test.entries_);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;
        EXPECT_EQ(storage.ents_.size(), test.wentries_.size()) << "#test case: " << i;
        if (storage.ents_.size() != test.wentries_.size()) {
            continue;
        }

        for (int i = 0; i < storage.ents_.size(); i++) {
            EXPECT_EQ(storage.ents_[i].index(), test.wentries_[i].index()) << "#test case: " << i;
            EXPECT_EQ(storage.ents_[i].term(), test.wentries_[i].term()) << "#test case: " << i;
        }
    }
}

TEST_F(MemoryStorageTest, ApplySnapshot) {
    raftpb::ConfState cs;
    cs.add_voters(1);
    cs.add_voters(2);
    cs.add_voters(3);

    std::string data("data");

    auto make_snapshot = [&cs, &data](uint64_t index, uint64_t term){
        raftpb::Snapshot snapshot;
        snapshot.set_data(data);
        snapshot.mutable_metadata()->set_index(index);
        snapshot.mutable_metadata()->set_term(term);
        *(snapshot.mutable_metadata()->mutable_conf_state()) = cs;

        return snapshot;
    };

    struct Test
    {
        raftpb::Snapshot snapshot_;
        craft::Status wstatus_;
    };

    std::vector<Test> tests = {
        {make_snapshot(4, 4), craft::Status::OK()},
        {make_snapshot(3, 3), craft::Status::Error("requested index is older than the existing snapshot [ms_index: 4, snap_index: 3]")}
    };
    
    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        craft::Status status = storage_.ApplySnapshot(test.snapshot_);
        EXPECT_STREQ(status.Str(), test.wstatus_.Str()) << "#test case: " << i;
    }
}

int _tmain(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}