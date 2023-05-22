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
#include <cstring>
#include <cassert>
#include <memory>

#include "gtest/gtest.h"
#include "log.h"

static craft::EntryPtr makeEntry(uint64_t index, uint64_t term) {
  auto ent = std::make_shared<raftpb::Entry>();
  ent->set_index(index);
  ent->set_term(term);
  return ent;
}

static bool IsEqual(const craft::EntryPtrs& a, const craft::EntryPtrs& b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i]->index() != b[i]->index()) {
      return false;
    }
    if (a[i]->term() != b[i]->term()) {
      return false;
    }
  }
  return true;
}

TEST(RaftLog, FindConflict) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)};
  struct Test {
    craft::EntryPtrs ents;
    uint64_t wconflict;
  };
  std::vector<Test> tests = {
    // no confilct, empty ent
    {{}, 0},
    // no conflict
    {{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}, 0},
    {{makeEntry(2, 2), makeEntry(3, 3)}, 0},
    {{makeEntry(3, 3)}, 0},
    // no confilict, but has new entries
    {{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4},
    {{makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4},
    {{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4},
    // conflicts with existing entries
    {{makeEntry(1, 4), makeEntry(2, 4)}, 1},
    {{makeEntry(2, 1), makeEntry(3, 4), makeEntry(4, 4)}, 2},
    {{makeEntry(3, 1), makeEntry(4, 2), makeEntry(5, 4), makeEntry(6, 4)}, 3}
  };

  for (auto& tt : tests) {
    auto raft_log = craft::RaftLog::New(std::make_shared<craft::MemoryStorage>());
    raft_log->Append(previous_ents);

    auto gconfilct = raft_log->FindConflict(tt.ents);
    ASSERT_EQ(gconfilct, tt.wconflict);
  }
}

TEST(RaftLog, IsUpToData) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)};
  auto raft_log = craft::RaftLog::New(std::make_shared<craft::MemoryStorage>());
  raft_log->Append(previous_ents);

  struct Test {
      uint64_t last_index;
      uint64_t term;
      bool wup_to_data;
  };  
  std::vector<Test> tests = {
      // greater term, ignore lastIndex
      {raft_log->LastIndex() - 1, 4, true},
      {raft_log->LastIndex(), 4, true},
      {raft_log->LastIndex() + 1, 4, true},
      // smaller term, ignore lastIndex
      {raft_log->LastIndex() - 1, 2, false},
      {raft_log->LastIndex(), 2, false},
      {raft_log->LastIndex() + 1, 2, false},
      // equal term, equal or lager lastIndex wins
      {raft_log->LastIndex() - 1, 3, false},
      {raft_log->LastIndex(), 3, true},
      {raft_log->LastIndex() + 1, 3, true}
  };

  for (auto& tt : tests) {
    bool gup_to_data = raft_log->IsUpToData(tt.last_index, tt.term);
    ASSERT_EQ(gup_to_data, tt.wup_to_data);
  }
}

TEST(RaftLog, Append) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2)};

  struct Test {
    craft::EntryPtrs ents;
    uint64_t windex;
    craft::EntryPtrs wents;
    uint64_t wunstable;
  };
  std::vector<Test> tests = {
    {{}, 2, {makeEntry(1, 1), makeEntry(2, 2)}, 3},
    {{makeEntry(3, 2)}, 3, {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 2)}, 3},
    // conflicts with index 1
    {{makeEntry(1, 2)}, 1, {makeEntry(1, 2)}, 1},
    // conflicts with index 2
    {{makeEntry(2, 3), makeEntry(3, 3)}, 3, {makeEntry(1, 1), makeEntry(2, 3), makeEntry(3, 3)}, 2}
  };

  for (auto& tt : tests) {
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->Append(previous_ents);
    auto raft_log = craft::RaftLog::New(storage);

    auto index = raft_log->Append(tt.ents);
    ASSERT_EQ(index, tt.windex);

    auto [g, s] = raft_log->Entries(1, craft::RaftLog::kNoLimit);
    ASSERT_TRUE(s.IsOK());
    ASSERT_TRUE(IsEqual(g, tt.wents));
    ASSERT_EQ(raft_log->GetUnstable().Offset(), tt.wunstable);
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
TEST(RaftLog, MaybeAppend) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)};
  uint64_t lastindex = 3;
  uint64_t lastterm = 3;
  uint64_t commit = 1;

  struct Test {
    uint64_t log_term;
    uint64_t index;
    uint64_t committed;
    craft::EntryPtrs ents;

    uint64_t wlasti;
    bool wappend;
    uint64_t wcommit;
  };
  std::vector<Test> tests = {
      // not match: term is different
      {
          lastterm - 1, lastindex, lastindex, {makeEntry(lastindex + 1, 4)},
          0, false, commit
      },
      // not match: index out of bound
      {
          lastterm, lastindex + 1, lastindex, {makeEntry(lastindex + 2, 4)},
          0, false, commit
      },
      // match with the last existing entry
      {
          lastterm, lastindex, lastindex, {},
          lastindex, true, lastindex
      },
      {
          lastterm, lastindex, lastindex + 1, {},
          lastindex, true, lastindex // do not increase commit higher than lastnewi
      },
      {
          lastterm, lastindex, lastindex - 1, {},
          lastindex, true, lastindex - 1 // commit up to the commit in the message
      },
      {
          lastterm, lastindex, 0, {},
          lastindex, true, commit // commit do not decrease
      },
      {
          0, 0, lastindex, {},
          0, true, commit // commit do not decrease
      },
      {
          lastterm, lastindex, lastindex, {makeEntry(lastindex + 1, 4)},
          lastindex + 1, true, lastindex
      },
      {
          lastterm, lastindex, lastindex + 1, {makeEntry(lastindex + 1, 4)},
          lastindex + 1, true, lastindex + 1
      },
      {
          lastterm, lastindex, lastindex + 2, {makeEntry(lastindex + 1, 4)},
          lastindex + 1, true, lastindex + 1 // do not increase commit higher than lastnewi
      },
      {
          lastterm, lastindex, lastindex + 2, {makeEntry(lastindex + 1, 4), makeEntry(lastindex + 2, 4)},
          lastindex + 2, true, lastindex + 2
      },
      // match with the the entry in the middle
      {
          lastterm - 1, lastindex - 1, lastindex, {makeEntry(lastindex, 4)},
          lastindex, true, lastindex
      },
      {
          lastterm - 2, lastindex - 2, lastindex, {makeEntry(lastindex - 1, 4)},
          lastindex - 1, true, lastindex - 1
      },
      {
          lastterm - 2, lastindex - 2, lastindex, {makeEntry(lastindex - 1, 4), makeEntry(lastindex, 4)},
          lastindex, true, lastindex
      },
      // panic
      // {
      //     lastterm - 3, lastindex - 3, lastindex, {makeEntry(lastindex - 2, 4)},
      //     lastindex - 2, true, lastindex - 2 // conflict with existing committed entry
      // }
  };

  for (auto& tt : tests) {
    auto raft_log = craft::RaftLog::New(std::make_shared<craft::MemoryStorage>());
    raft_log->Append(previous_ents);
    raft_log->SetCommitted(commit);

    auto [glasti, gappend] = raft_log->MaybeAppend(tt.index, tt.log_term, tt.committed, tt.ents);
    auto gcommit = raft_log->Committed();
    ASSERT_EQ(glasti, tt.wlasti);
    ASSERT_EQ(gappend, tt.wappend);
    ASSERT_EQ(gcommit, tt.wcommit);
    if (gappend && !tt.ents.empty()) {
      auto [gents, s] = raft_log->Slice(raft_log->LastIndex()-tt.ents.size()+1, raft_log->LastIndex()+1, craft::RaftLog::kNoLimit);
      ASSERT_TRUE(s.IsOK());
      ASSERT_TRUE(IsEqual(tt.ents, gents));
    }
  }
}

// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
TEST(RaftLog, CompactionSideEffects) {
  // Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
  uint64_t last_index = 1000;
  uint64_t unstable_index = 750;
  uint64_t last_term = last_index;
  auto storage = std::make_shared<craft::MemoryStorage>();
  for (uint64_t i = 1; i <= unstable_index; i++) {
    storage->Append({makeEntry(i, i)});
  }

  auto raft_log = craft::RaftLog::New(storage);
  for (uint64_t i = unstable_index; i < last_index; i++) {
    raft_log->Append({makeEntry(i+1, i+1)});
  }

  auto ok = raft_log->MaybeCommit(last_index, last_term);
  ASSERT_TRUE(ok);

  uint64_t offset = 500;
  storage->Compact(offset);

  ASSERT_EQ(raft_log->LastIndex(), last_index);

  for (auto j = offset; j < raft_log->LastIndex(); j++) {
    auto [term, s] = raft_log->Term(j);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(term, j);
  }

  for (auto j = offset; j < raft_log->LastIndex(); j++) {
    auto ok = raft_log->MatchTerm(j, j);
    ASSERT_TRUE(ok);
  }

  auto unstable_ents = raft_log->UnstableEntries();
  ASSERT_EQ(unstable_ents.size(), static_cast<size_t>(250));
  ASSERT_EQ(unstable_ents[0]->index(), static_cast<uint64_t>(751));

  auto prev = raft_log->LastIndex();
  raft_log->Append({makeEntry(raft_log->LastIndex() + 1, raft_log->LastIndex() + 1)});
  ASSERT_EQ(raft_log->LastIndex(), prev + 1);

  auto [ents, s] = raft_log->Entries(raft_log->LastIndex(), craft::RaftLog::kNoLimit);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(ents.size(), static_cast<size_t>(1));
}

TEST(RaftLog, HasNextEnts) {
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_term(1);
  snap->mutable_metadata()->set_index(3);

  craft::EntryPtrs ents = {makeEntry(4, 1), makeEntry(5, 1), makeEntry(6, 1)};

  struct Test {
      uint64_t applied;
      bool whas_next;
  };

  std::vector<Test> tests = {
    {0, true},
    {3, true},
    {4, true},
    {5, false}
  };

  for (auto& tt : tests) {
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->ApplySnapshot(snap);
    auto raft_log = craft::RaftLog::New(storage);
    raft_log->Append(ents);
    raft_log->MaybeCommit(5, 1);
    raft_log->AppliedTo(tt.applied);
    auto has_next = raft_log->HasNextEnts();
    ASSERT_EQ(has_next, tt.whas_next);
  }
}

TEST(RaftLog, NextEnts) {
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_term(1);
  snap->mutable_metadata()->set_index(3);

  craft::EntryPtrs ents = {makeEntry(4, 1), makeEntry(5, 1), makeEntry(6, 1)};
  
  struct Test {
    uint64_t applied;
    craft::EntryPtrs wents;
  };

  std::vector<Test> tests = {
    {0, {makeEntry(4, 1), makeEntry(5, 1)}},
    {3, {makeEntry(4, 1), makeEntry(5, 1)}},
    {4, {makeEntry(5, 1)}},
    {5, {}}
  };

  for (auto& tt : tests) {
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->ApplySnapshot(snap);
    auto raft_log = craft::RaftLog::New(storage);
    raft_log->Append(ents);
    raft_log->MaybeCommit(5, 1);
    raft_log->AppliedTo(tt.applied);

    auto nents = raft_log->NextEnts();
    ASSERT_TRUE(IsEqual(nents, tt.wents));
  }
}

static craft::EntryPtrs getEnts(const craft::EntryPtrs& entries, size_t b, size_t e = UINT32_MAX) {
  craft::EntryPtrs ents;
  if (b >= entries.size()) {
    return ents;
  }
  e = std::min(e, entries.size());
  for (size_t i = b; i < e; i++) {
    ents.emplace_back(entries[i]);
  }
  return ents;
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
TEST(RaftLog, UnstableEnts) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2)};
  
  struct Test {
    uint64_t unstable;
    craft::EntryPtrs wents;
  };

  std::vector<Test> tests = {
    {3, {}},
    {1, previous_ents}
  };

  for (auto& tt : tests) {
    // append stable entries to storage
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->Append(getEnts(previous_ents, 0, tt.unstable - 1));

    // append unstable entries to raftlog
    auto raft_log = craft::RaftLog::New(storage);
    raft_log->Append(getEnts(previous_ents, tt.unstable - 1));

    auto ents = raft_log->UnstableEntries();
    if (!ents.empty()) {
      auto l = ents.size();
      raft_log->StableTo(ents[l-1]->index(), ents[l-1]->term());
    }
    ASSERT_TRUE(IsEqual(ents, tt.wents));
    auto w = previous_ents[previous_ents.size()-1]->index() + 1;
    auto g = raft_log->GetUnstable().Offset();
    ASSERT_EQ(g, w);
  }
}

TEST(RaftLog, CommitTo) {
  craft::EntryPtrs previous_ents = {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)};
  uint64_t commit = 2;
  
  struct Test {
    uint64_t commit;
    uint64_t wcommit;
  };

  std::vector<Test> tests = {
    {3, 3},
    {1, 2} // never decrease
    // {4, 0} // commit out of range -> panic
  };

  for (auto& tt : tests) {
    auto raft_log = std::make_shared<craft::RaftLog>(std::make_shared<craft::MemoryStorage>());
    raft_log->Append(previous_ents);
    raft_log->SetCommitted(commit);
    raft_log->CommitTo(tt.commit);
    ASSERT_EQ(raft_log->Committed(), tt.wcommit);
  }
}

TEST(RaftLog, StableTo) {
  craft::EntryPtrs ents = {makeEntry(1, 1), makeEntry(2, 2)};

  struct Test {
    uint64_t stablei;
    uint64_t stablet;
    uint64_t wunstable;
  };

  std::vector<Test> tests = {
    {1, 1, 2},
    {2, 2, 3},
    {2, 1, 1}, // bad term
    {3, 1, 1}  // bad index
  };

  for (auto&& tt : tests) {
    auto raft_log = std::make_shared<craft::RaftLog>(std::make_shared<craft::MemoryStorage>());
    raft_log->Append({makeEntry(1, 1), makeEntry(2, 2)});
    raft_log->StableTo(tt.stablei, tt.stablet);
    ASSERT_EQ(raft_log->GetUnstable().Offset(), tt.wunstable);
  }
}

TEST(RaftLog, StableToWithSnap) {
  uint64_t snapi = 5;
  uint64_t snapt = 2;

  struct Test {
    uint64_t stablei;
    uint64_t stablet;
    craft::EntryPtrs new_ents;

    uint64_t wunstable;
  };
  std::vector<Test> tests = {
    {snapi+1, snapt, {}, snapi+1},
    {snapi, snapt, {}, snapi+1},
    {snapi-1, snapt, {}, snapi+1},

    {snapi+1, snapt+1, {}, snapi+1},
    {snapi, snapt+1, {}, snapi+1},
    {snapi-1, snapt+1, {}, snapi+1},

    {snapi+1, snapt, {makeEntry(snapi+1, snapt)}, snapi+2},
    {snapi, snapt, {makeEntry(snapi+1, snapt)}, snapi+1},
    {snapi-1, snapt, {makeEntry(snapi+1, snapt)}, snapi+1},

    {snapi+1, snapt+1, {makeEntry(snapi+1, snapt)}, snapi+1},
    {snapi, snapt+1, {makeEntry(snapi+1, snapt)}, snapi+1},
    {snapi-1, snapt+1, {makeEntry(snapi+1, snapt)}, snapi+1}
  };

  for (auto& tt : tests) {
    auto s = std::make_shared<craft::MemoryStorage>();
    auto snap = std::make_shared<raftpb::Snapshot>();
    snap->mutable_metadata()->set_index(snapi);
    snap->mutable_metadata()->set_term(snapt);
    s->ApplySnapshot(snap);
    auto raft_log = craft::RaftLog::New(s);
    raft_log->Append(tt.new_ents);
    raft_log->StableTo(tt.stablei, tt.stablet);
    ASSERT_EQ(raft_log->GetUnstable().Offset(), tt.wunstable);
  }
}

//TestCompaction ensures that the number of log entries is correct after compactions.
TEST(RaftLog, Compaction) {
  struct Test {
    uint64_t last_index;
    std::vector<uint64_t> compact;
    std::vector<int> wleft;
    bool wallow;
  };

  std::vector<Test> tests = {
    // out of upper bound
    // {1000, {1001}, {-1}, false}, // panic
    {1000, {300, 500, 800, 900}, {700, 500, 200, 100}, true},
    // out of lower bound
    {1000, {300, 299}, {700, -1}, false}
  };

  for (auto& tt : tests) {
    auto storage = std::make_shared<craft::MemoryStorage>();
    for (uint64_t i = 1; i <= tt.last_index; i++) {
      storage->Append({makeEntry(i, 0)});
    }
    auto raft_log = craft::RaftLog::New(storage);
    raft_log->MaybeCommit(tt.last_index, 0);
    raft_log->AppliedTo(raft_log->Committed());

    for (size_t j = 0; j < tt.compact.size(); j++) {
      auto status = storage->Compact(tt.compact[j]);
      if (!status.IsOK()) {
          ASSERT_FALSE(tt.wallow);
          continue;
      }
      ASSERT_EQ(raft_log->AllEntries().size(), static_cast<size_t>(tt.wleft[j]));
    }
  }
}

TEST(RaftLog, LogRestore) {
  uint64_t index = 1000;
  uint64_t term = 1000;
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(index);
  snap->mutable_metadata()->set_term(term);

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->ApplySnapshot(snap);

  auto raft_log = craft::RaftLog::New(storage);

  ASSERT_EQ(raft_log->AllEntries().size(), static_cast<size_t>(0));
  ASSERT_EQ(raft_log->FirstIndex(), index + 1);
  ASSERT_EQ(raft_log->Committed(), index);
  ASSERT_EQ(raft_log->GetUnstable().Offset(), index + 1);
  auto [gterm, s] = raft_log->Term(index);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(gterm, term);
}

TEST(RaftLog, IsOutOfBounds) {
  uint64_t offset = 100;
  uint64_t num = 100;
  auto storage = std::make_shared<craft::MemoryStorage>();
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(offset);
  storage->ApplySnapshot(snap);
  auto l = craft::RaftLog::New(storage);
  for (uint64_t i = 1; i <= num; i++) {
    l->Append({makeEntry(i + offset, 0)});
  }

  uint64_t first = offset + 1;

  struct Test {
    uint64_t lo;
    uint64_t hi;
    
    bool wpanic;
    bool werr_compacted;
  };

  std::vector<Test> tests = {
    {first - 1, first + 1, false, true},
    {first - 1, first + 1, false, true},
    {first, first, false, false},
    {first + num / 2, first + num / 2, false, false},
    {first + num - 1, first + num - 1, false, false},
    {first + num, first + num, false, false}
    // panic
    // {first+num, first+num+1, true, false},
    // {first+num+1, first+num+1, true, false}
  };

  for (auto& tt : tests) {
    auto status = l->MustCheckOutOfBounds(tt.lo, tt.hi);
    ASSERT_FALSE(tt.wpanic);
    if (tt.werr_compacted) {
      ASSERT_STREQ(status.Str(), craft::kErrCompacted);
    }
    if (!tt.werr_compacted) {
      ASSERT_TRUE(status.IsOK());
    }
  }
}

TEST(RaftLog, Term) {
  uint64_t offset = 100;
  uint64_t num = 100;

  auto storage = std::make_shared<craft::MemoryStorage>();
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(offset);
  snap->mutable_metadata()->set_term(1);
  storage->ApplySnapshot(snap);
  auto l = craft::RaftLog::New(storage);
  for (uint64_t i = 1; i < num; i++) {
    l->Append({makeEntry(offset + i, i)});
  }

  struct Test {
    uint64_t index;
    uint64_t w;
  };
  std::vector<Test> tests = {
		{offset - 1, 0},
		{offset, 1},
		{offset + num / 2, num / 2},
		{offset + num - 1, num - 1},
		{offset + num, 0},
  };
  for (auto& tt : tests) {
    auto [term, s] = l->Term(tt.index);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(term, tt.w);
  }
}

TEST(RaftLog, TermWithUnstableSnapshot) {
  uint64_t storagesnapi = 100;
  uint64_t unstablesnapi = storagesnapi + 5;

  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(storagesnapi);
  snap->mutable_metadata()->set_term(1);
  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->ApplySnapshot(snap);

  auto snap2 = std::make_shared<raftpb::Snapshot>();
  snap2->mutable_metadata()->set_index(unstablesnapi);
  snap2->mutable_metadata()->set_term(1);
  auto l = craft::RaftLog::New(storage);
  l->Restore(snap2);

  struct Test {
    uint64_t index;
    uint64_t w;
  };
  std::vector<Test> tests = {
    // cannot get term from storage
    {storagesnapi, 0},
    // cannot get term from the gap between storage ents and unstable snapshot
    {storagesnapi + 1, 0},
    {unstablesnapi - 1, 0},
    // get term from unstable snapshot index
    {unstablesnapi, 1},
  };
  for (auto& tt : tests) {
    auto [term, s] = l->Term(tt.index);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(term, tt.w);
  }
}

TEST(RaftLog, Slice) {
  uint64_t i;
  uint64_t offset = 100;
  uint64_t num = 100;
  uint64_t last = offset + num;
  uint64_t half = offset + num / 2;
  auto halfe = std::make_shared<raftpb::Entry>();
  halfe->set_index(half);
  halfe->set_term(half);

  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(offset);
  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->ApplySnapshot(snap);
  for (i = 1; i < num / 2; i++) {
    storage->Append({makeEntry(offset + i, offset + i)});
  }

  auto l = craft::RaftLog::New(storage);
  for (i = num / 2; i < num; i++) {
    l->Append({makeEntry(offset + i, offset + i)});
  }

  struct Test {
    uint64_t from;
    uint64_t to;
    uint64_t limit;

    craft::EntryPtrs w;
    bool panic;
  };
  std::vector<Test> tests = {
		// test no limit
		{offset - 1, offset + 1, craft::RaftLog::kNoLimit, {}, false},
		{offset, offset + 1, craft::RaftLog::kNoLimit, {}, false},
		{half - 1, half + 1, craft::RaftLog::kNoLimit, {makeEntry(half - 1, half - 1), makeEntry(half, half)}, false},
		{half, half + 1, craft::RaftLog::kNoLimit, {makeEntry(half, half)}, false},
		{last - 1, last, craft::RaftLog::kNoLimit, {makeEntry(last - 1, last - 1)}, false},
		// {last, last + 1, craft::RaftLog::kNoLimit, {}, true},

		// test limit
		{half - 1, half + 1, 0, {makeEntry(half - 1, half - 1)}, false},
		{half - 1, half + 1, uint64_t(halfe->ByteSizeLong() + 1), {makeEntry(half - 1, half - 1)}, false},
		{half - 2, half + 1, uint64_t(halfe->ByteSizeLong() + 1), {makeEntry(half - 2, half - 2)}, false},
		{half - 1, half + 1, uint64_t(halfe->ByteSizeLong() * 2), {makeEntry(half - 1, half - 1), makeEntry(half, half)}, false},
		{half - 1, half + 2, uint64_t(halfe->ByteSizeLong() * 3), {makeEntry(half - 1, half - 1), makeEntry(half, half), makeEntry(half + 1, half + 1)}, false},
		{half, half + 2, uint64_t(halfe->ByteSizeLong()), {makeEntry(half, half)}, false},
		{half, half + 2, uint64_t(halfe->ByteSizeLong() * 2), {makeEntry(half, half), makeEntry(half + 1, half + 1)}, false},
  };

  for (auto& tt : tests) {
    auto [g, s] = l->Slice(tt.from, tt.to, tt.limit);
    if (tt.from <= offset) {
      ASSERT_STREQ(s.Str(), craft::kErrCompacted);
    }
    if (tt.from > offset) {
      ASSERT_TRUE(s.IsOK());
    }
    ASSERT_TRUE(IsEqual(g, tt.w));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}