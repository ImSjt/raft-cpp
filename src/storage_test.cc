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
#include "src/storage.h"

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

static bool IsEqual(std::shared_ptr<raftpb::Snapshot> a, std::shared_ptr<raftpb::Snapshot> b) {
  if (a->data() != b->data()) {
    return false;
  }
  if (a->metadata().index() != b->metadata().index()) {
    return false;
  }
  if (a->metadata().term() != b->metadata().term()) {
    return false;
  }

  auto& avoters = a->metadata().conf_state().voters();
  auto& bvoters = b->metadata().conf_state().voters();
  if (avoters.size() != bvoters.size()) {
    return false;
  }
  for (int i = 0; i < avoters.size(); i++) {
    if (avoters[i] != bvoters[i]) {
      return false;
    }
  }
  return true;
}

TEST(MemoryStorage, Term) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  struct Test {
    uint64_t index;

    uint64_t wterm;
    craft::Status wstatus;
  };

  std::vector<Test> tests = {
    {2, 0, craft::Status::Error(craft::kErrCompacted)},
    {3, 3, craft::Status::OK()},
    {4, 4, craft::Status::OK()},
    {5, 5, craft::Status::OK()},
    {6, 0, craft::Status::Error(craft::kErrUnavailable)}
  };

  for (auto& tt : tests) {
    craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
    s.SetEntries(ents);

    auto [term, status] = s.Term(tt.index);
    ASSERT_EQ(status.IsOK(), tt.wstatus.IsOK());
    if (!status.IsOK()) {
      ASSERT_STREQ(status.Str(), tt.wstatus.Str());
    }
    ASSERT_EQ(term, tt.wterm);
  }
}

TEST(MemoryStorage, Entries) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)};
  struct Test {
    uint64_t lo;
    uint64_t hi;
    uint64_t max_size;

    craft::Status wstatus;
    craft::EntryPtrs wentries;
  };

  std::vector<Test> tests = {
    {2, 6, std::numeric_limits<uint64_t>::max(), craft::Status::Error(craft::kErrCompacted), {}},
    {3, 4, std::numeric_limits<uint64_t>::max(), craft::Status::Error(craft::kErrCompacted), {}},
    {4, 5, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {makeEntry(4, 4)}},
    {4, 6, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5)}},
    {4, 7, std::numeric_limits<uint64_t>::max(), craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
    // even if maxsize is zero, the first entry should be returned
    {4, 7, 0, craft::Status::OK(), {makeEntry(4, 4)}},
    // limit to 2
    {4, 7, ents[1]->ByteSizeLong() + ents[2]->ByteSizeLong(), craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5)}},
    // limit to 2
    {4, 7, ents[1]->ByteSizeLong() + ents[2]->ByteSizeLong() + ents[3]->ByteSizeLong() / 2, craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5)}},
    {4, 7, ents[1]->ByteSizeLong() + ents[2]->ByteSizeLong() + ents[3]->ByteSizeLong() - 1, craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5)}},
    // all
    {4, 7, ents[1]->ByteSizeLong() + ents[2]->ByteSizeLong() + ents[3]->ByteSizeLong(), craft::Status::OK(), {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
  };

  for (auto& tt : tests) {
    craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
    s.SetEntries(ents);
    auto [entries, status] = s.Entries(tt.lo, tt.hi, tt.max_size);
    ASSERT_EQ(status.IsOK(), tt.wstatus.IsOK());
    if (!status.IsOK()) {
      ASSERT_STREQ(status.Str(), tt.wstatus.Str());
    }
    ASSERT_TRUE(IsEqual(entries, tt.wentries));
  }
}

TEST(MemoryStorage, LastIndex) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
  s.SetEntries(ents);

  auto [last, status] = s.LastIndex();
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(last, static_cast<uint64_t>(5));

  s.Append({makeEntry(6, 5)});
  std::tie(last, status) = s.LastIndex();
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(last, static_cast<uint64_t>(6));;
}

TEST(MemoryStorage, FirstIndex) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
  s.SetEntries(ents);

  auto [first, status] = s.FirstIndex();
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(first, static_cast<uint64_t>(4));

  s.Compact(4);
  std::tie(first, status) = s.FirstIndex();
  ASSERT_TRUE(status.IsOK());
  EXPECT_EQ(first, static_cast<uint64_t>(5));
}

TEST(MemoryStorage, Compact) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  struct Test {
    uint64_t i;

    craft::Status wstatus;
    uint64_t windex;
    uint64_t wterm;
    uint64_t wlen;
  };

  std::vector<Test> tests = {
    {2, craft::Status::Error(craft::kErrCompacted), 3, 3, 3},
    {3, craft::Status::Error(craft::kErrCompacted), 3, 3, 3},
    {4, craft::Status::OK(), 4, 4, 2},
    {5, craft::Status::OK(), 5, 5, 1}
  };

  for (auto& tt : tests) {
    craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
    s.SetEntries(ents);
    auto status = s.Compact(tt.i);
    ASSERT_EQ(status.IsOK(), tt.wstatus.IsOK());
    if (!status.IsOK()) {
      ASSERT_STREQ(status.Str(), tt.wstatus.Str());
    }
    
    ASSERT_EQ(s.GetEntries()[0]->index(), tt.windex);
    ASSERT_EQ(s.GetEntries()[0]->term(), tt.wterm);
    ASSERT_EQ(s.GetEntries().size(), tt.wlen);
  }
}

TEST(MemoryStorage, CreateSnapshot) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  raftpb::ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);
  std::string data("data");
  auto make_snapshot = [&cs, &data](uint64_t index, uint64_t term) {
    auto snapshot = std::make_shared<raftpb::Snapshot>();
    snapshot->set_data(data);
    snapshot->mutable_metadata()->set_index(index);
    snapshot->mutable_metadata()->set_term(term);
    *(snapshot->mutable_metadata()->mutable_conf_state()) = cs;
    return snapshot;
  };

  struct Test {
    uint64_t i;
    craft::Status wstatus;
    std::shared_ptr<raftpb::Snapshot> wsnap;
  };

  std::vector<Test> tests = {
    {4, craft::Status::OK(), make_snapshot(4, 4)},
    {5, craft::Status::OK(), make_snapshot(5, 5)},
  };

  for (auto& tt : tests) {
    craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
    s.SetEntries(ents);
    auto [snap, status] = s.CreateSnapshot(tt.i, &cs, data);

    ASSERT_EQ(status.IsOK(), tt.wstatus.IsOK());
    if (!status.IsOK()) {
      ASSERT_STREQ(status.Str(), tt.wstatus.Str());
    }
    ASSERT_TRUE(IsEqual(snap, tt.wsnap));
  }
}

TEST(MemoryStorage, Append) {
  craft::EntryPtrs ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)};
  struct Test {
    craft::EntryPtrs entries;
    craft::Status wstatus;
    craft::EntryPtrs wentries;
  };

  std::vector<Test> tests = {
    {{makeEntry(1, 1), makeEntry(2, 2)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}},
    {{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}},
    {{makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)}},
    {{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5),
      makeEntry(6, 5)}},
    // truncate incoming entries, truncate the existing entries and append
    {{makeEntry(2, 3), makeEntry(3, 3), makeEntry(4, 5)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 5)}},
    {{makeEntry(4, 5)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 5)}},
    // direct append
    {{makeEntry(6, 5)},
      craft::Status::OK(),
      {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5),
      makeEntry(6, 5)}}
    };

  for (auto& tt : tests) {
    craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());
    s.SetEntries(ents);
    auto status = s.Append(tt.entries);
    ASSERT_EQ(status.IsOK(), tt.wstatus.IsOK());
    if (!status.IsOK()) {
      ASSERT_STREQ(status.Str(), tt.wstatus.Str());
    }
    ASSERT_TRUE(IsEqual(s.GetEntries(), tt.wentries));
  }
}

TEST(MemoryStorage, ApplySnapshot) {
  raftpb::ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);
  std::string data("data");

  auto make_snapshot = [&cs, &data](uint64_t index, uint64_t term) {
    auto snapshot = std::make_shared<raftpb::Snapshot>();
    snapshot->set_data(data);
    snapshot->mutable_metadata()->set_index(index);
    snapshot->mutable_metadata()->set_term(term);
    *(snapshot->mutable_metadata()->mutable_conf_state()) = cs;
    return snapshot;
  };

  craft::MemoryStorage s(std::make_shared<craft::ConsoleLogger>());

	//Apply Snapshot successful
  auto status = s.ApplySnapshot(make_snapshot(4, 4));
  ASSERT_TRUE(status.IsOK());

	//Apply Snapshot fails due to ErrSnapOutOfDate
  status = s.ApplySnapshot(make_snapshot(3, 3));
  ASSERT_FALSE(status.IsOK());
  ASSERT_STREQ(status.Str(), craft::kErrSnapOutOfDate);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}