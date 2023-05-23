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
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/tracker/progress.h"

TEST(Progress, String) {
  auto ins = std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 1);
  ins->Add(123);
  craft::Progress p(std::make_shared<craft::ConsoleLogger>());
  p.SetMatch(1);
  p.SetNext(2);
  p.SetState(craft::StateType::kSnapshot);
  p.SetPendingSnapshot(123);
  p.SetRecentActive(false);
  p.SetProbeSent(true);
  p.SetIsLearner(true);
  p.SetInflights(std::move(ins));
  std::string exp =
      "StateSnapshot match=1 next=2 learner paused pending_snap=123 inactive "
      "inflight=1[full]";
  ASSERT_EQ(p.String(), exp);
}

TEST(Progress, IsPaused) {
  struct Test {
    std::string name;
    craft::StateType state;
    bool paused;

    bool w;
  };

  std::vector<Test> tests = {
    {"test0", craft::kProbe, false, false},
    {"test1", craft::kProbe, true, true},
    {"test2", craft::kReplicate, false, false},
    {"test3", craft::kReplicate, true, false},
    {"test4", craft::kSnapshot, false, true},
    {"test5", craft::kSnapshot, true, true}
  };

  for (auto& test : tests) {
    craft::Progress p(std::make_shared<craft::ConsoleLogger>());
    p.SetState(test.state);
    p.SetProbeSent(test.paused);
    p.SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));
    bool g = p.IsPaused();
    ASSERT_EQ(g, test.w);
  }
}

// TestProgressResume ensures that MaybeUpdate and MaybeDecrTo will reset
// ProbeSent.
TEST(Progress, Resume) {
  craft::Progress p(std::make_shared<craft::ConsoleLogger>());
  p.SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));
  p.SetNext(2);
  p.SetMatch(0);
  p.SetProbeSent(true);
  p.MaybeDecrTo(1, 1);
  ASSERT_FALSE(p.ProbeSent()) << "paused= " << p.ProbeSent() << ", want false";
  p.SetProbeSent(true);
  p.MaybeUpdate(2);
  ASSERT_FALSE(p.ProbeSent()) << "paused= " << p.ProbeSent() << ", want false";
}

TEST(Progress, BecomeProbe) {
  struct Test {
    craft::ProgressPtr p;
    uint64_t wnext;
  };

  uint64_t match = 1;
  std::vector<Test> tests;
  {
    auto p = std::make_shared<craft::Progress>(std::make_shared<craft::ConsoleLogger>());
    p->SetState(craft::StateType::kReplicate);
    p->SetMatch(match);
    p->SetNext(5);
    p->SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));
    tests.emplace_back(Test{
      .p = p,
      .wnext = 2,
    });
  }
  {
    // snapshot finish
    auto p = std::make_shared<craft::Progress>(std::make_shared<craft::ConsoleLogger>());
    p->SetState(craft::StateType::kSnapshot);
    p->SetMatch(match);
    p->SetNext(5);
    p->SetPendingSnapshot(10);
    p->SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));
    tests.emplace_back(Test{
      .p = p,
      .wnext = 11,
    });
  }

  {
    // snapshot failure
    auto p = std::make_shared<craft::Progress>(std::make_shared<craft::ConsoleLogger>());
    p->SetState(craft::StateType::kSnapshot);
    p->SetMatch(match);
    p->SetNext(5);
    p->SetPendingSnapshot(0);
    p->SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));
    tests.emplace_back(Test{
      .p = p,
      .wnext = 2,
    });
  }

  for (auto& test : tests) {
    test.p->BecomeProbe();
    ASSERT_EQ(test.p->State(), craft::StateType::kProbe);
    ASSERT_EQ(test.p->Match(), match);
    ASSERT_EQ(test.p->Next(), test.wnext);
  }
}

TEST(Progress, BecomeReplicate) {
  craft::Progress p(std::make_shared<craft::ConsoleLogger>());
  p.SetState(craft::StateType::kProbe);
  p.SetMatch(1);
  p.SetNext(5);
  p.SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));

  p.BecomeReplicate();

  ASSERT_EQ(p.State(), craft::StateType::kReplicate);
  ASSERT_EQ(p.Match(), static_cast<uint64_t>(1));
  ASSERT_EQ(p.Next(), p.Match() + 1);
}

TEST(Progress, BecomeSnapshot) {
  craft::Progress p(std::make_shared<craft::ConsoleLogger>());
  p.SetState(craft::StateType::kProbe);
  p.SetMatch(1);
  p.SetNext(5);
  p.SetInflights(std::make_unique<craft::Inflights>(std::make_shared<craft::ConsoleLogger>(), 256));

  p.BecomeSnapshot(10);

  ASSERT_EQ(p.State(), craft::StateType::kSnapshot);
  ASSERT_EQ(p.Match(), static_cast<uint64_t>(1));
  ASSERT_EQ(p.PendingSnapshot(), static_cast<uint64_t>(10));
}

TEST(Progress, MaybeUpdate) {
  uint64_t prev_m = 3;
  uint64_t prev_n = 5;
  struct Test {
    uint64_t update;

    uint64_t wm;
    uint64_t wn;
    bool wok;
  };
  std::vector<Test> tests = {
    {prev_m - 1, prev_m, prev_n, false},     // do not decrease match, next
		{prev_m, prev_m, prev_n, false},            // do not decrease next
		{prev_m + 1, prev_m + 1, prev_n, true},     // increase match, do not decrease next
		{prev_m + 2, prev_m + 2, prev_n + 1, true}, // increase match, next
  };
  for (auto& tt : tests) {
    craft::Progress p(std::make_shared<craft::ConsoleLogger>());
    p.SetMatch(prev_m);
    p.SetNext(prev_n);
    auto ok = p.MaybeUpdate(tt.update);
    ASSERT_EQ(ok, tt.wok);
    ASSERT_EQ(p.Match(), tt.wm);
    ASSERT_EQ(p.Next(), tt.wn);
  }
}

TEST(Progress, MaybeDecrTo) {
  struct Test {
    craft::StateType state;
    uint64_t m;
    uint64_t n;
    uint64_t rejected;
    uint64_t last;

    bool w;
    uint64_t wn;
  };
  std::vector<Test> tests = {
		{
			// state replicate and rejected is not greater than match
			craft::StateType::kReplicate, 5, 10, 5, 5, false, 10,
		},
		{
			// state replicate and rejected is not greater than match
			craft::StateType::kReplicate, 5, 10, 4, 4, false, 10,
		},
		{
			// state replicate and rejected is greater than match
			// directly decrease to match+1
			craft::StateType::kReplicate, 5, 10, 9, 9, true, 6,
		},
		{
			// next-1 != rejected is always false
			craft::StateType::kProbe, 0, 0, 0, 0, false, 0,
		},
		{
			// next-1 != rejected is always false
			craft::StateType::kProbe, 0, 10, 5, 5, false, 10,
		},
		{
			// next>1 = decremented by 1
			craft::StateType::kProbe, 0, 10, 9, 9, true, 9,
		},
		{
			// next>1 = decremented by 1
			craft::StateType::kProbe, 0, 2, 1, 1, true, 1,
		},
		{
			// next<=1 = reset to 1
			craft::StateType::kProbe, 0, 1, 0, 0, true, 1,
		},
		{
			// decrease to min(rejected, last+1)
			craft::StateType::kProbe, 0, 10, 9, 2, true, 3,
		},
		{
			// rejected < 1, reset to 1
			craft::StateType::kProbe, 0, 10, 9, 0, true, 1,
		},
  };
  for (auto& tt : tests) {
    craft::Progress p(std::make_shared<craft::ConsoleLogger>());
    p.SetState(tt.state);
    p.SetMatch(tt.m);
    p.SetNext(tt.n);
    auto g = p.MaybeDecrTo(tt.rejected, tt.last);
    ASSERT_EQ(g, tt.w);
    ASSERT_EQ(p.Match(), tt.m);
    ASSERT_EQ(p.Next(), tt.wn);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}