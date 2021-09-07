#include "gtest/gtest.h"

#include <cassert>
#include <memory>

#define private public
#define protected public
#include "craft/log.h"
#undef private
#undef protected

static raftpb::Entry make_entry(uint64_t index, uint64_t term);

class RaftLogTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<raftpb::Entry> previous_ents = {make_entry(1, 1), make_entry(2, 2), make_entry(3, 3)};
        std::shared_ptr<craft::Storage> storage_empty(new craft::MemoryStorage);
        raft_log_.reset(new craft::RaftLog(storage_empty));
        raft_log_->Append(previous_ents);
    }

    // void TearDown() override {}

protected:
    std::shared_ptr<craft::RaftLog> raft_log_;

};

static raftpb::Entry make_entry(uint64_t index, uint64_t term) {
    raftpb::Entry ent;
    
    ent.set_index(index);
    ent.set_term(term);

    return ent;
}

TEST_F(RaftLogTest, FindConflict) {
    struct Test {
        std::vector<raftpb::Entry> ents_;
        uint64_t wconflict_;
    };
    std::vector<Test> tests = {
        // no confilct, empty ent
        {{}, 0},
        // no conflict
        {{make_entry(1, 1), make_entry(2, 2), make_entry(3, 3)}, 0},
        {{make_entry(2, 2), make_entry(3, 3)}, 0},
        {{make_entry(3, 3)}, 0},
        // no confilict, but has new entries
        {{make_entry(1, 1), make_entry(2, 2), make_entry(3, 3), make_entry(4, 4), make_entry(5, 4)}, 4},
        {{make_entry(2, 2), make_entry(3, 3), make_entry(4, 4), make_entry(5, 4)}, 4},
        {{make_entry(3, 3), make_entry(4, 4), make_entry(5, 4)}, 4},
        // conflicts with existing entries
        {{make_entry(1, 4), make_entry(2, 4)}, 1},
        {{make_entry(2, 1), make_entry(3, 4), make_entry(4, 4)}, 2},
        {{make_entry(3, 1), make_entry(4, 2), make_entry(5, 4), make_entry(6, 4)}, 3}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        uint64_t gconflict = raft_log_->FindConflict(test.ents_);
        EXPECT_EQ(gconflict, test.wconflict_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, IsUpToData) {
    struct Test {
        uint64_t last_index_;
        uint64_t term_;
        bool wup_to_data_;
    };
    
    std::vector<Test> tests = {
        // greater term, ignore lastIndex
        {raft_log_->LastIndex() - 1, 4, true},
        {raft_log_->LastIndex(), 4, true},
        {raft_log_->LastIndex() + 1, 4, true},
        // smaller term, ignore lastIndex
        {raft_log_->LastIndex() - 1, 2, false},
        {raft_log_->LastIndex(), 2, false},
        {raft_log_->LastIndex() + 1, 2, false},
        // equal term, equal or lager lastIndex wins
        {raft_log_->LastIndex() - 1, 3, false},
        {raft_log_->LastIndex(), 3, true},
        {raft_log_->LastIndex() + 1, 3, true}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        bool gup_to_data = raft_log_->IsUpToData(test.last_index_, test.term_);
        EXPECT_EQ(gup_to_data, test.wup_to_data_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, Append) {
    std::vector<raftpb::Entry> previous_ents = {make_entry(1, 1), make_entry(2, 2)};

    struct Test {
        std::vector<raftpb::Entry> ents_;
        uint64_t windex_;
        std::vector<raftpb::Entry> wents_;
        uint64_t wunstable_;
    };
    std::vector<Test> tests = {
        {
            {},
            2,
            {make_entry(1, 1), make_entry(2, 2)},
            3
        },
        {
            {make_entry(3, 2)},
            3,
            {make_entry(1, 1), make_entry(2, 2), make_entry(3, 2)},
            3
        },
        // conflicts with index 1
        {
            {make_entry(1, 2)},
            1,
            {make_entry(1, 2)},
            1
        },
        // conflicts with index 2
        {
            {make_entry(2, 3), make_entry(3, 3)},
            3,
            {make_entry(1, 1), make_entry(2, 3), make_entry(3, 3)},
            2
        }
    };
    
    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];

        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        craft::Status status = storage->Append(previous_ents);
        assert(status.ok());
        craft::RaftLog raft_log(storage);

        uint64_t index = raft_log.Append(test.ents_);
        EXPECT_EQ(index, test.windex_) << "#test case: " << i;

        std::vector<raftpb::Entry> ents;
        status = raft_log.Entries(1, craft::RaftLog::kNoLimit, ents);
        EXPECT_TRUE(status.ok()) << "#test case: " << i << ", unexpected error: " << status.Str();
        if (!status.ok()) {
            continue;
        }

        EXPECT_EQ(ents.size(), test.wents_.size()) << "#test case: " << i;
        if (ents.size() != test.wents_.size()) {
            continue;
        }

        for (int j = 0; j < ents.size(); j++) {
            EXPECT_EQ(ents[j].index(), test.wents_[j].index()) << "#test case: " << i;
            EXPECT_EQ(ents[j].term(), test.wents_[j].term()) << "#test case: " << i;
        }

        uint64_t goff = raft_log.unstable_.offset_;
        EXPECT_EQ(goff, test.wunstable_) << "#test case: " << i;
    }
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
// 	1. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it
// 	2.Append any new entries not already in the log
// If the given (index, term) does not match with the existing log:
// 	return false
TEST_F(RaftLogTest, MaybeAppend) {
    uint64_t lastindex = 3;
    uint64_t lastterm = 3;
    uint64_t commit = 1;

    struct Test {
        uint64_t log_term_;
        uint64_t index_;
        uint64_t committed_;
        std::vector<raftpb::Entry> ents_;

        uint64_t wlasti_;
        uint64_t wcommit_;
    };
    std::vector<Test> tests = {
        // not match: term is different
        {
            lastterm - 1, lastindex, lastindex, {make_entry(lastindex + 1, 4)},
            0, commit
        },
        // not match: index out of bound
        {
            lastterm, lastindex + 1, lastindex, {make_entry(lastindex + 2, 4)},
            0, commit
        },
        // match with the last existing entry
        {
            lastterm, lastindex, lastindex, {},
            lastindex, lastindex
        },
        {
            lastterm, lastindex, lastindex + 1, {},
            lastindex, lastindex // do not increase commit higher than lastnewi
        },
        {
            lastterm, lastindex, lastindex - 1, {},
            lastindex, lastindex - 1 // commit up to the commit in the message
        },
        {
            lastterm, lastindex, 0, {},
            lastindex, commit // commit do not decrease
        },
        {
            0, 0, lastindex, {},
            0, commit // commit do not decrease
        },
        {
            lastterm, lastindex, lastindex, {make_entry(lastindex + 1, 4)},
            lastindex + 1, lastindex
        },
        {
            lastterm, lastindex, lastindex + 1, {make_entry(lastindex + 1, 4)},
            lastindex + 1, lastindex + 1
        },
        {
            lastterm, lastindex, lastindex + 2, {make_entry(lastindex + 1, 4)},
            lastindex + 1, lastindex + 1 // do not increase commit higher than lastnewi
        },
        {
            lastterm, lastindex, lastindex + 2, {make_entry(lastindex + 1, 4), make_entry(lastindex + 2, 4)},
            lastindex + 2, lastindex + 2
        },
        // match with the the entry in the middle
        {
            lastterm - 1, lastindex - 1, lastindex, {make_entry(lastindex, 4)},
            lastindex, lastindex
        },
        {
            lastterm - 2, lastindex - 2, lastindex, {make_entry(lastindex - 1, 4)},
            lastindex - 1, lastindex - 1
        },
        {
            lastterm - 2, lastindex - 2, lastindex, {make_entry(lastindex - 1, 4), make_entry(lastindex, 4)},
            lastindex, lastindex
        }
        // panic
        // {
        //     lastterm - 3, lastindex - 3, lastindex, {make_entry(lastindex - 2, 4)},
        //     lastindex - 2, lastindex - 2 // conflict with existing committed entry
        // }
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        raft_log_->committed_ = commit;

        uint64_t glasti = raft_log_->MaybeAppend(test.index_, test.log_term_, test.committed_, test.ents_);
        EXPECT_EQ(glasti, test.wlasti_) << "#test case: " << i;

        uint64_t gcommit = raft_log_->committed_;
        EXPECT_EQ(gcommit, test.wcommit_) << "#test case: " << i;

        if (glasti == 0 || test.ents_.empty()) {
            continue;
        }

        std::vector<raftpb::Entry> gents;
        uint64_t lo = raft_log_->LastIndex() - static_cast<uint64_t>(test.ents_.size()) + 1;
        craft::Status status = raft_log_->Slice(lo, raft_log_->LastIndex()+1, craft::RaftLog::kNoLimit, gents);
        EXPECT_TRUE(status.ok()) << "#test case: " << i;
        if (!status.ok()) {
            continue;
        }

        EXPECT_EQ(gents.size(), test.ents_.size()) << "#test case: " << i;
        if (gents.size() != test.ents_.size()) {
            continue;
        }

        for (int i = 0; i < gents.size(); i++) {
            EXPECT_EQ(gents[i].index(), test.ents_[i].index()) << "#test case: " << i;
            EXPECT_EQ(gents[i].term(), test.ents_[i].term()) << "#test case: " << i;
        }
    }
}

// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
TEST_F(RaftLogTest, CompactionSideEffects) {
    // Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
    uint64_t last_index = 1000;
    uint64_t unstable_index = 750;
    uint64_t last_term = last_index;
    std::vector<raftpb::Entry> ents;

    std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
    ents.clear();
    for (int i = 1; i <= unstable_index; i++) {
        ents.push_back(make_entry(i, i));
    }
    storage->Append(ents);

    std::unique_ptr<craft::RaftLog> raft_log(new craft::RaftLog(storage));
    ents.clear();
    for (int i = unstable_index; i < last_index; i++) {
        ents.push_back(make_entry(i+1, i+1));
    }
    raft_log->Append(ents);

    bool ok = raft_log->MaybeCommit(last_index, last_term);
    ASSERT_TRUE(ok);

    raft_log->AppliedTo(raft_log->committed_);
    
    uint64_t offset = 500;
    craft::Status status = storage->Compact(offset);
    ASSERT_TRUE(status.ok()) << "unexpected error: " << status.Str();
    ASSERT_EQ(raft_log->LastIndex(), last_index);
    
    for (uint64_t j = offset; j <= raft_log->LastIndex(); j++) {
        uint64_t term;
        status = raft_log->Term(j, term);
        ASSERT_TRUE(status.ok()) << "unexpected error: " << status.Str();
        ASSERT_EQ(term, j);
    }
    
    for (uint64_t j = offset; j <= raft_log->LastIndex(); j++) {
        ASSERT_TRUE(raft_log->MatchTerm(j, j)) << "j=" << j;
    }

    ents.clear();
    raft_log->UnstableEntries(ents);
    ASSERT_EQ(ents.size(), 250);
    ASSERT_EQ(ents[0].index(), unstable_index+1);

    uint64_t prev = raft_log->LastIndex();
    ents.clear();
    ents.push_back(make_entry(raft_log->LastIndex() + 1, raft_log->LastIndex() + 1));
    raft_log->Append(ents);
    ASSERT_EQ(raft_log->LastIndex(), prev+1);

    ents.clear();
    status = raft_log->Entries(raft_log->LastIndex(), craft::RaftLog::kNoLimit, ents);
    ASSERT_TRUE(status.ok()) << "unexpected error: " << status.Str();
    ASSERT_TRUE(ents.size() == 1);
}

TEST_F(RaftLogTest, HasNextEnts) {
    raftpb::Snapshot snap;
    snap.mutable_metadata()->set_term(1);
    snap.mutable_metadata()->set_index(3);

    std::vector<raftpb::Entry> ents = {make_entry(4, 1), make_entry(5, 1), make_entry(6, 1)};

    struct Test {
        uint64_t applied_;
        bool whas_next_;
    };

    std::vector<Test> tests = {
        {0, true},
        {3, true},
        {4, true},
        {5, false}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        storage->ApplySnapshot(snap);
        craft::RaftLog raft_log(storage);
        raft_log.Append(ents);
        raft_log.MaybeCommit(5, 1);
        raft_log.AppliedTo(test.applied_);

        bool has_next = raft_log.HasNextEnts();
        EXPECT_EQ(has_next, test.whas_next_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, NextEnts) {
    raftpb::Snapshot snap;
    snap.mutable_metadata()->set_term(1);
    snap.mutable_metadata()->set_index(3);

    std::vector<raftpb::Entry> ents = {make_entry(4, 1), make_entry(5, 1), make_entry(6, 1)};
    
    struct Test {
        uint64_t applied_;
        std::vector<raftpb::Entry> wents_;
    };

    std::vector<Test> tests = {
        {0, {make_entry(4, 1), make_entry(5, 1)}},
        {3, {make_entry(4, 1), make_entry(5, 1)}},
        {4, {make_entry(5, 1)}},
        {5, {}}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        storage->ApplySnapshot(snap);
        craft::RaftLog raft_log(storage);
        raft_log.Append(ents);
        raft_log.MaybeCommit(5, 1);
        raft_log.AppliedTo(test.applied_);

        std::vector<raftpb::Entry> nents;
        raft_log.NextEnts(nents);
        EXPECT_EQ(nents.size(), test.wents_.size()) << "#test case: " << i;
        if (nents.size() != test.wents_.size()) {
            continue;
        }

        for (int i = 0; i < nents.size(); i++) {
            EXPECT_EQ(nents[i].index(), test.wents_[i].index()) << "#test case: " << i;
            EXPECT_EQ(nents[i].term(), test.wents_[i].term()) << "#test case: " << i;
        }
    }
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
TEST_F(RaftLogTest, UnstableEnts) {
    std::vector<raftpb::Entry> previous_ents = {make_entry(1, 1), make_entry(2, 2)};
    
    struct Test {
        uint64_t unstable_;
        std::vector<raftpb::Entry> wents_;
    };

    std::vector<Test> tests = {
        {3, {}},
        {1, previous_ents}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        // append stable entries to storage
        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        std::vector<raftpb::Entry> ents_s(previous_ents.begin(), previous_ents.begin()+(test.unstable_-1));
        storage->Append(ents_s);

        craft::RaftLog raft_log(storage);
        std::vector<raftpb::Entry> ents_u(previous_ents.begin()+(test.unstable_-1), previous_ents.end());
        raft_log.Append(ents_u);

        std::vector<raftpb::Entry> ents;
        raft_log.UnstableEntries(ents);
        if (!ents.empty()) {
            raft_log.StableTo(ents.rbegin()->index(), ents.rbegin()->term());
        }

        EXPECT_EQ(ents.size(), test.wents_.size()) << "#test case: " << i;
        if (ents.size() != test.wents_.size()) {
            continue;
        }

        for (int i = 0; i < ents.size(); i++) {
            EXPECT_EQ(ents[i].index(), test.wents_[i].index()) << "#test case: " << i;
            EXPECT_EQ(ents[i].term(), test.wents_[i].term()) << "#test case: " << i;
        }

        uint64_t w = previous_ents.rbegin()->index() + 1;
        EXPECT_EQ(w, raft_log.unstable_.offset_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, CommitTo) {
    std::vector<raftpb::Entry> previous_ents = {make_entry(1, 1), make_entry(2, 2), make_entry(3, 3)};
    uint64_t commit = 2;
    
    struct Test {
        uint64_t commit_;
        uint64_t wcommit_;
    };

    std::vector<Test> tests = {
        {3, 3},
        {1, 2} // never decrease
        // {4, 0} // commit out of range -> panic
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        raft_log_->committed_ = commit;
        raft_log_->CommitTo(test.commit_);
        EXPECT_EQ(raft_log_->committed_, test.wcommit_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, StableTo) {
    std::vector<raftpb::Entry> ents = {make_entry(1, 1), make_entry(2, 2)};

    struct Test {
        uint64_t stablei_;
        uint64_t stablet_;
        uint64_t wunstable_;
    };

    std::vector<Test> tests = {
        {1, 1, 2},
        {2, 2, 3},
        {2, 1, 1}, // bad term
        {3, 1, 1}  // bad index
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        craft::RaftLog raft_log(storage);
        raft_log.Append(ents);
        raft_log.StableTo(test.stablei_, test.stablet_);

        EXPECT_EQ(raft_log.unstable_.offset_, test.wunstable_) << "#test case: " << i;
    }
}

TEST_F(RaftLogTest, StableToWithSnap) {
    uint64_t snapi = 5;
    uint64_t snapt = 2;

    struct Test {
        uint64_t stablei_;
        uint64_t stablet_;
        std::vector<raftpb::Entry> new_ents_;

        uint64_t wunstable_;
    };
    std::vector<Test> tests = {
        {snapi+1, snapt, {}, snapi+1},
        {snapi, snapt, {}, snapi+1},
        {snapi-1, snapt, {}, snapi+1},

        {snapi+1, snapt+1, {}, snapi+1},
        {snapi, snapt+1, {}, snapi+1},
        {snapi-1, snapt+1, {}, snapi+1},

        {snapi+1, snapt, {make_entry(snapi+1, snapt)}, snapi+2},
        {snapi, snapt, {make_entry(snapi+1, snapt)}, snapi+1},
        {snapi-1, snapt, {make_entry(snapi+1, snapt)}, snapi+1},

        {snapi+1, snapt+1, {make_entry(snapi+1, snapt)}, snapi+1},
        {snapi, snapt+1, {make_entry(snapi+1, snapt)}, snapi+1},
        {snapi-1, snapt+1, {make_entry(snapi+1, snapt)}, snapi+1}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        raftpb::Snapshot snap;
        snap.mutable_metadata()->set_term(snapt);
        snap.mutable_metadata()->set_index(snapi);

        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        storage->ApplySnapshot(snap);

        craft::RaftLog raft_log(storage);
        raft_log.Append(test.new_ents_);
        raft_log.StableTo(test.stablei_, test.stablet_);
        EXPECT_EQ(raft_log.unstable_.offset_, test.wunstable_) << "#test case: " << i;
    }
}

//TestCompaction ensures that the number of log entries is correct after compactions.
TEST_F(RaftLogTest, Compaction) {
    struct Test {
        uint64_t last_index_;
        std::vector<uint64_t> compact_;
        std::vector<int> wleft_;
        bool wallow_;
    };

    std::vector<Test> tests = {
        // out of upper bound
        // {1000, {1001}, {-1}, false}, // panic
        {1000, {300, 500, 800, 900}, {700, 500, 200, 100}, true},
        // out of lower bound
        {1000, {300, 299}, {700, -1}, false}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
        std::vector<raftpb::Entry> ents;
        for (int j = 1; j <= test.last_index_; j++) {
            ents.push_back(make_entry(j, 0));
        }
        storage->Append(ents);

        craft::RaftLog raft_log(storage);
        raft_log.MaybeCommit(test.last_index_, 0);
        raft_log.AppliedTo(raft_log.committed_);

        for (int j = 0; j < test.compact_.size(); j++) {
            craft::Status status = storage->Compact(test.compact_[j]);
            if (!status.ok()) {
                EXPECT_TRUE(!test.wallow_) << "#test case: " << i << ", unexpected error: " << status.Str();
                continue;
            }

            std::vector<raftpb::Entry> all_ents;
            raft_log.AllEntries(all_ents);
            EXPECT_EQ(all_ents.size(), test.wleft_[j]) << "#test case: " << i;
        }
    }
}

TEST_F(RaftLogTest, LogRestore) {
    uint64_t index = 1000;
    uint64_t term = 1000;
    raftpb::Snapshot snap;
    snap.mutable_metadata()->set_term(term);
    snap.mutable_metadata()->set_index(index);

    std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
    storage->ApplySnapshot(snap);

    craft::RaftLog raft_log(storage);

    std::vector<raftpb::Entry> ents;
    raft_log.AllEntries(ents);
    EXPECT_TRUE(ents.empty());

    EXPECT_EQ(raft_log.FirstIndex(), index+1);
    EXPECT_EQ(raft_log.committed_, index);
    EXPECT_EQ(raft_log.unstable_.offset_, index+1);
    
    uint64_t gterm;
    craft::Status status = raft_log.Term(index, gterm);
    ASSERT_TRUE(status.ok()) << "unexpected error: " << status.Str();
    EXPECT_EQ(gterm, term);
}

TEST_F(RaftLogTest, IsOutOfBounds) {
    uint64_t offset = 100;
    uint64_t num = 100;

    raftpb::Snapshot snap;
    snap.mutable_metadata()->set_term(0);
    snap.mutable_metadata()->set_index(offset);
    
    std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
    storage->ApplySnapshot(snap);
    
    craft::RaftLog raft_log(storage);
    std::vector<raftpb::Entry> ents;
    for (uint64_t i = 1; i <= num; i++) {
        ents.push_back(make_entry(i+offset, 0));
    }
    raft_log.Append(ents);

    uint64_t first = offset + 1;

    struct Test {
        uint64_t lo;
        uint64_t hi;
        
        bool wpanic;
        bool werr_compacted;
    };

    std::vector<Test> tests = {
        {first-1, first+1, false, true},
        {first-1, first+1, false, true},
        {first, first, false, false},
        {first+num/2, first+num/2, false, false},
        {first+num-1, first+num-1, false, false},
        {first+num, first+num, false, false}
        // panic
        // {first+num, first+num+1, true, false},
        // {first+num+1, first+num+1, true, false}
    };

    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        craft::Status status = raft_log.MustCheckOutOfBounds(test.lo, test.hi);
        
        if (test.werr_compacted) {
            EXPECT_TRUE(std::strstr(status.Str(), craft::kErrCompacted) != nullptr) << "#test case: " << i << ", unexpected error: " << status.Str();
        } else {
            EXPECT_TRUE(status.ok()) << "#test case: " << i << ", unexpected error: " << status.Str();
        }
    }

}

TEST_F(RaftLogTest, Term) {
    uint64_t offset = 100;
    uint64_t num = 100;

    raftpb::Snapshot snap;
    snap.mutable_metadata()->set_term(1);
    snap.mutable_metadata()->set_index(offset);
    
    std::shared_ptr<craft::Storage> storage(new craft::MemoryStorage);
    storage->ApplySnapshot(snap);

    craft::RaftLog raft_log(storage);
    std::vector<raftpb::Entry> ents;
    for (uint64_t i = 1; i < num; i++) {
        ents.push_back(make_entry(offset+i, i));
    }
    raft_log.Append(ents);

    struct Test {
        uint64_t index_;
        uint64_t wterm_;
    };

    std::vector<Test> tests = {
        {offset-1, 0},
        {offset, 1},
        {offset+num/2, num/2},
        {offset+num-1, num-1},
        {offset+num, 0}
    };
    
    for (int i = 0; i < tests.size(); i++) {
        Test& test = tests[i];
        uint64_t term;
        craft::Status status = raft_log.Term(test.index_, term);
        EXPECT_TRUE(status.ok()) << "#test case: " << i << ", unexpected error: " << status.Str();
        EXPECT_EQ(term, test.wterm_) << "#test case: " << i;
    }
}