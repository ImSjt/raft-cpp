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
#include "gtest/gtest.h"
#include "src/log_unstable.h"

static craft::EntryPtr makeEntry(uint64_t index, uint64_t term) {
  auto ent = std::make_shared<raftpb::Entry>();
  ent->set_index(index);
  ent->set_term(term);
  return ent;
}

static craft::SnapshotPtr makeSnapshot(uint64_t index, uint64_t term) {
  auto snapshot = std::make_shared<raftpb::Snapshot>();
  snapshot->mutable_metadata()->set_index(index);
  snapshot->mutable_metadata()->set_term(term);
  return snapshot;
}

TEST(Unstable, MaybeFirstIndex) {
  struct Test {
    craft::EntryPtrs entries;
    uint64_t offset;
    craft::SnapshotPtr snap;

    bool wok;
    uint64_t windex;
  };

  std::vector<Test> tests = {
    // no snapshot
    {
      {makeEntry(5, 1)}, 5, nullptr, false, 0,
    },
    {
      {}, 0, nullptr, false, 0,
    },
    // has snapshot
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), true, 5,
    },
    {
      {}, 5, makeSnapshot(4, 1), true, 5,
    }
  };

  for (auto& tt : tests) {
    craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
    u.SetEntries(tt.entries);
    u.SetOffset(tt.offset);
    u.SetSnapshot(tt.snap);
    auto [index, ok] = u.MaybeFirstIndex();
    ASSERT_EQ(ok, tt.wok);
    ASSERT_EQ(index, tt.windex);
  }
}

TEST(Unstable, MaybeLastIndex) {
  struct Test {
    craft::EntryPtrs entries;
    uint64_t offset;
    craft::SnapshotPtr snap;

    bool wok;
    uint64_t windex;
  };

  std::vector<Test> tests = {
    // last in entries
    {
      {makeEntry(5, 1)}, 5, nullptr, true, 5,
    },
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), true, 5,
    },
    // last in snapshot
    {
      {}, 5, makeSnapshot(4, 1), true, 4,
    },
    // empty unstable
    {
      {}, 0, nullptr, false, 0,
    }
  };

  for (auto& tt : tests) {
    craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
    u.SetEntries(tt.entries);
    u.SetOffset(tt.offset);
    u.SetSnapshot(tt.snap);
    auto [index, ok] = u.MaybeLastIndex();
    ASSERT_EQ(ok, tt.wok);
    ASSERT_EQ(index, tt.windex);
  }
}

TEST(Unstable, MaybeTerm) {
  struct Test {
    craft::EntryPtrs entries;
    uint64_t offset;
    craft::SnapshotPtr snap;
    uint64_t index;

    bool wok;
    uint64_t wterm;
  };

  std::vector<Test> tests = {
    // term from entries
    {
      {makeEntry(5, 1)}, 5, nullptr, 5, true, 1,
    },
    {
      {makeEntry(5, 1)}, 5, nullptr, 6, false, 0,
    },
    {
      {makeEntry(5, 1)}, 5, nullptr, 4, false, 0,
    },
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 5, true, 1,
    },
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 6, false, 0,
    },
    // term from snapshot
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 4, true, 1,
    },
    {
      {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 3, false, 0,
    },
    {
      {}, 5, makeSnapshot(4, 1), 5, false, 0,
    },
    {
      {}, 5, makeSnapshot(4, 1), 4, true, 1,
    },
    {
      {}, 0, nullptr, 5, false, 0,
    }
  };

  for (auto& tt : tests) {
    craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
    u.SetEntries(tt.entries);
    u.SetOffset(tt.offset);
    u.SetSnapshot(tt.snap);

    auto [term, ok] = u.MaybeTerm(tt.index);
    ASSERT_EQ(ok, tt.wok);
    ASSERT_EQ(term, tt.wterm);
  }
}

TEST(Unstable, Restore) {
  craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
  u.SetEntries({makeEntry(4, 1)});
  u.SetOffset(5);
  u.SetSnapshot(makeSnapshot(4, 1));

  auto s = makeSnapshot(6, 2);
  u.Restore(s);

  ASSERT_EQ(u.Offset(), s->metadata().index() + 1);
  ASSERT_TRUE(u.Entries().empty());
  ASSERT_TRUE(u.Snapshot() != nullptr);
  ASSERT_EQ(u.Snapshot()->metadata().index(), s->metadata().index());
  ASSERT_EQ(u.Snapshot()->metadata().term(), s->metadata().term());
}

TEST(Unstable, StableTo) {
  struct Test {
    craft::EntryPtrs entries;
    uint64_t offset;
    craft::SnapshotPtr snap;
    uint64_t index;
    uint64_t term;

    uint64_t woffset;
    uint64_t wlen;
  };

  std::vector<Test> tests = {
      {
        {}, 0, nullptr, 5, 1, 0, 0
      },
      // stable to the first entry
      {
        {makeEntry(5, 1)}, 5, nullptr, 5, 1, 6, 0
      },
      // stable to the first entry
      {
        {makeEntry(5, 1), makeEntry(6, 1)}, 5, nullptr, 5, 1, 6, 1
      },
      // stable to the first entry and term mismatch
      {
        {makeEntry(6, 2)}, 6, nullptr, 6, 1, 6, 1
      },
      // stable to old entry
      {
        {makeEntry(5, 1)}, 5, nullptr, 4, 1, 5, 1
      },
      // with snapshot
      // stable to the first entry
      {
        {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 5, 1, 6, 0
      },
      // stable to the first entry
      {
        {makeEntry(5, 1), makeEntry(6, 1)}, 5, makeSnapshot(4, 1), 5, 1, 6, 1
       },
      // stable to the first entry and term mismatch
      {
        {makeEntry(6, 2)}, 6, makeSnapshot(5, 1), 6, 1, 6, 1
      },
      // stable to snapshot
      {
        {makeEntry(5, 1)}, 5, makeSnapshot(4, 1), 4, 1, 5, 1
      },
      // stable to old entry
      {
        {makeEntry(5, 2)}, 5, makeSnapshot(4, 2), 4, 1, 5, 1
      }
    };

  for (auto& tt : tests) {
    craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
    u.SetEntries(tt.entries);
    u.SetOffset(tt.offset);
    u.SetSnapshot(tt.snap);

    u.StableTo(tt.index, tt.term);
    ASSERT_EQ(u.Offset(), tt.woffset);
    ASSERT_EQ(u.Entries().size(), tt.wlen);
  }
}

TEST(Unstable, TruncateAndAppend) {
  struct Test {
    craft::EntryPtrs entries;
    uint64_t offset;
    craft::SnapshotPtr snap;
    craft::EntryPtrs toappend;

    uint64_t woffset;
    craft::EntryPtrs wentries;
  };

  std::vector<Test> tests = {
    // append to the end
    {
      {makeEntry(5, 1)},
      5, nullptr,
      {makeEntry(6, 1), makeEntry(7, 1)},
      5,
      {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)}},
    // replace the unstable entries
    {
      {makeEntry(5, 1)},
      5, nullptr,
      {makeEntry(5, 2), makeEntry(6, 2)},
      5,
      {makeEntry(5, 2), makeEntry(6, 2)}},
    {
      {makeEntry(5, 1)},
      5, nullptr,
      {makeEntry(4, 2), makeEntry(5, 2), makeEntry(6, 2)},
      4,
      {makeEntry(4, 2), makeEntry(5, 2), makeEntry(6, 2)}},
    // truncate the existing entries and append
    {
      {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)},
      5, nullptr,
      {makeEntry(6, 2)},
      5,
      {makeEntry(5, 1), makeEntry(6, 2)}},
    {
      {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)},
      5, nullptr,
      {makeEntry(7, 2), makeEntry(8, 2)},
      5,
      {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 2), makeEntry(8, 2)}
    }
  };

  for (auto& tt : tests) {
    craft::Unstable u(std::make_shared<craft::ConsoleLogger>());
    u.SetEntries(tt.entries);
    u.SetOffset(tt.offset);
    u.SetSnapshot(tt.snap);

    u.TruncateAndAppend(tt.toappend);

    ASSERT_EQ(u.Offset(), tt.woffset);
    ASSERT_EQ(u.Entries().size(), tt.wentries.size());
    for (size_t i = 0; i < u.Entries().size(); i++) {
      ASSERT_EQ(u.Entries()[i]->index(), tt.wentries[i]->index());
      ASSERT_EQ(u.Entries()[i]->term(), tt.wentries[i]->term());
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}