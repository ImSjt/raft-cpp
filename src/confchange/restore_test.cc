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
#include <random>

#include "gtest/gtest.h"
#include "confchange/confchange.h"
#include "confchange/restore.h"
#include "raftpb/confstate.h"

static std::random_device rd;
static std::mt19937 gen(rd());
 
static int random(int low, int high) {
  std::uniform_int_distribution<> dist(low, high);
  return dist(gen);
}

static std::vector<uint64_t> genRandomArray(int n) {
  std::vector<uint64_t> m(n);
  for (int i = 0; i < n; i++) {
    int j = random(0, i);
    m[i] = m[j];
    m[j] = i;
  }
  return m;
}

static auto makeRepeatedField(std::vector<uint64_t> arr) {
  ::google::protobuf::RepeatedField< ::google::protobuf::uint64 > m;
  for (auto i : arr) {
    m.Add(i);
  }
  return m;
}

static std::vector<uint64_t> slice(std::vector<uint64_t>& m, size_t num) {
  std::vector<uint64_t> n;
  if (num > m.size()) {
    return n;
  }
  size_t count = 0;
  for (auto it = m.begin(); it != m.end();) {
    n.push_back(*it);
    it = m.erase(it);
    count++;
    if (count >= num) {
      break;
    }
  }
  return n;
}

static raftpb::ConfState genConfState() {
  raftpb::ConfState cs;
  // NB: never generate the empty ConfState, that one should be unit tested.
  auto nvoters = 1 + random(0, 4);

  auto nlearners = random(0, 4);

	// The number of voters that are in the outgoing config but not in the
	// incoming one. (We'll additionally retain a random number of the
	// incoming voters below).
  auto nremoved_voters = random(0, 2);

	// Voters, learners, and removed voters must not overlap. A "removed voter"
	// is one that we have in the outgoing config but not the incoming one.
  auto ids = genRandomArray(2 * (nvoters + nlearners + nremoved_voters));
  for (auto& id : ids) {
    id += 1;
  }

  *cs.mutable_voters() = makeRepeatedField(slice(ids, nvoters));

  if (nlearners > 0) {
    *cs.mutable_learners() = makeRepeatedField(slice(ids, nlearners));
  }

	// Roll the dice on how many of the incoming voters we decide were also
	// previously voters.
	//
	// NB: this code avoids creating non-nil empty slices (here and below).
  auto noutgoing_retained_voters = random(0, nvoters);
  if (noutgoing_retained_voters > 0 || nremoved_voters > 0) {
    for (size_t i = 0; i < noutgoing_retained_voters; i++) {
      cs.add_voters_outgoing(cs.voters(i));
    }
    for (size_t i = 0; i < nremoved_voters; i++) {
      cs.add_voters_outgoing(ids[i]);
    }
  }
	// Only outgoing voters that are not also incoming voters can be in
	// LearnersNext (they represent demotions).
  if (nremoved_voters > 0) {
    auto nlearners_next = random(0, nremoved_voters);
    for (size_t i = 0; i < nlearners_next; i++) {
      cs.add_learners_next(ids[i]);
    }
  }
  cs.set_auto_leave(cs.voters_outgoing().size() > 0 && (random(0, 1) == 1));
  return cs;
}

static void printConfState(const raftpb::ConfState& cs) {
  std::cout << "=========" << std::endl;
  std::cout << "voters:";
  for (auto voter : cs.voters()) {
    std::cout << voter << " ";
  }
  std::cout << std::endl;

  std::cout << "learners:";
  for (auto learner : cs.learners()) {
    std::cout << learner << " ";
  }
  std::cout << std::endl;

  std::cout << "voters_outgoing:";
  for (auto voter : cs.voters_outgoing()) {
    std::cout << voter << " ";
  }
  std::cout << std::endl;

  std::cout << "learners_next:";
  for (auto learner : cs.learners_next()) {
    std::cout << learner << " ";
  }
  std::cout << std::endl;

  std::cout << "auto_leave:" << cs.auto_leave() << std::endl;
  std::cout << "=========" << std::endl;
}

TEST(Restore, Quick) {
  auto f = [](const raftpb::ConfState& cs) {
    craft::Changer chg(craft::ProgressTracker(20), 10);
    auto [cfg, prs, status] = craft::Restore(chg, cs);
    EXPECT_TRUE(status.IsOK()) << status.Str();
    chg.GetProgressTracker().SetConfig(std::move(cfg));
    chg.GetProgressTracker().SetProgressMap(std::move(prs));

    auto cs2 = chg.GetProgressTracker().ConfState();
    using namespace craft;
    EXPECT_TRUE(cs == cs2);
  };

  {
    raftpb::ConfState cs;
    f(cs);
  }
  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 2, 3});
    f(cs);
  }
  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 2, 3});
    *cs.mutable_learners() = makeRepeatedField({4, 5, 6});
    f(cs);
  }

  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 2, 3});
    *cs.mutable_learners() = makeRepeatedField({5});
    *cs.mutable_voters_outgoing() = makeRepeatedField({1, 2, 4, 6});
    *cs.mutable_learners_next() = makeRepeatedField({4});
    f(cs);
  }

  for (size_t i = 0; i < 5000; i++) {
    auto cs = genConfState();
    f(cs);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}