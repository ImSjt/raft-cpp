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
#include <map>
#include <memory>
#include <set>
#include <vector>
#include <random>

#include "gtest/gtest.h"
#include "src/confchange/confchange.h"
#include "src/util.h"

template <typename GN, typename GID, typename GT>
static std::vector<raftpb::ConfChangeSingle> genCC(GN&& num, GID&& id, GT&& typ) {
  std::vector<raftpb::ConfChangeSingle> ccs;
  auto n = num();
  for (int i = 0; i < n; i++) {
    raftpb::ConfChangeSingle cc;
    cc.set_type(typ());
    cc.set_node_id(id());
    ccs.emplace_back(cc);
  }
  return ccs;
}

static std::vector<raftpb::ConfChangeSingle> genInitialChanges() {
  auto num = []() {
    return 1 + craft::Util::Random(0, 4);
  };
  auto id = [&num]() {
    return static_cast<uint64_t>(num());
  };
  auto typ = []() {
    return raftpb::ConfChangeType::ConfChangeAddNode;
  };
	// NodeID one is special - it's in the initial config and will be a voter
	// always (this is to avoid uninteresting edge cases where the simple conf
	// changes can't easily make progress).
  std::vector<raftpb::ConfChangeSingle> init_ccs;
  raftpb::ConfChangeSingle cc;
  cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
  cc.set_node_id(1);
  init_ccs.emplace_back(cc);
  auto ccs = genCC(num, id, typ);
  for (auto& cc : ccs) {
    init_ccs.emplace_back(cc);
  }
  return init_ccs;
}

static std::vector<raftpb::ConfChangeSingle> genConfChanges() {
  auto num = []() {
    return 1 + craft::Util::Random(0, 8);
  };
  auto id = [&num]() {
		// Note that num() >= 1, so we're never returning 1 from this method,
		// meaning that we'll never touch NodeID one, which is special to avoid
		// voterless configs altogether in this test.
    return 1 + static_cast<uint64_t>(num());
  };
  auto typ = []() {
    return static_cast<raftpb::ConfChangeType>(craft::Util::Random(0, raftpb::ConfChangeType_ARRAYSIZE-1));
  };
  return genCC(num, id, typ);
}

static bool isEqual(const craft::ProgressMap& a, const craft::ProgressMap& b) {
  if (a.size() != b.size()) {
    return false;
  }
  auto ita = a.begin();
  auto itb = b.begin();
  for (; ita != a.end() && itb != b.end(); ita++, itb++) {
    if (ita->first != itb->first) {
      return false;
    }
    if (ita->second->String() != itb->second->String()) {
      return false;
    }
  }
  return true;
}

TEST(ConfChange, Quick) {
  using CCS = std::vector<raftpb::ConfChangeSingle>;
  auto run_with_joint = [](craft::Changer& c,
                           const CCS& ccs) {
    auto [cfg, prs, s1] = c.EnterJoint(false, ccs);
    EXPECT_TRUE(s1.IsOK()) << s1.Str();

		// Also do this with autoLeave on, just to check that we'd get the same
		// result.
    auto [cfg2a, prs2a, s2] = c.EnterJoint(true, ccs);
    EXPECT_TRUE(s2.IsOK()) << s2.Str();

    cfg2a.auto_leave = false;
    EXPECT_EQ(cfg, cfg2a);
    EXPECT_TRUE(isEqual(prs, prs2a));

    c.GetProgressTracker().SetConfig(cfg);
    c.GetProgressTracker().SetProgressMap(prs);
    auto [cfg2b, prs2b, s3] = c.LeaveJoint();
    EXPECT_TRUE(s3.IsOK()) << s3.Str();

    // Reset back to the main branch with autoLeave=false.
    c.GetProgressTracker().SetConfig(cfg);
    c.GetProgressTracker().SetProgressMap(prs);
    auto [cfg2c, prs2c, s4] = c.LeaveJoint();
    EXPECT_TRUE(s4.IsOK()) << s4.Str();

    EXPECT_EQ(cfg2b, cfg2c);
    EXPECT_TRUE(isEqual(prs2b, prs2c));

    c.GetProgressTracker().SetConfig(cfg2c);
    c.GetProgressTracker().SetProgressMap(prs2c);
    return craft::Status::OK();
  };

  auto run_with_simple = [](craft::Changer& c,
                            const CCS& ccs) {
    for (auto& cc : ccs) {
      auto [cfg, prs, status] = c.Simple({cc});
      if (!status.IsOK()) {
        return status;
      }
      c.GetProgressTracker().SetConfig(cfg);
      c.GetProgressTracker().SetProgressMap(prs);
    }
    return craft::Status::OK();
  };

  using TestFunc =
      std::function<craft::Status(craft::Changer& c, const CCS& ccs)>;
  auto wrapper = [&run_with_simple](TestFunc invoke) {
    return [&run_with_simple, invoke](
               const CCS& setup,
               const CCS& ccs) -> std::tuple<craft::Changer, craft::Status> {
      craft::ProgressTracker tr(10);
      craft::Changer c(std::make_shared<craft::ConsoleLogger>(), tr, 10);
      auto status = run_with_simple(c, setup);
      if (!status.IsOK()) {
        return std::make_tuple(c, std::move(status));
      }
      status = invoke(c, ccs);
      return std::make_tuple(std::move(c), std::move(status));
    };
  };

  auto f1 = [&wrapper, &run_with_simple](const CCS& setup, const CCS& ccs) {
    auto [c, s] = wrapper(run_with_simple)(setup, ccs);
    EXPECT_TRUE(s.IsOK()) << s.Str();
    return c;
  };

  auto f2 = [&wrapper, &run_with_joint](const CCS& setup, const CCS& ccs) {
    auto [c, s] = wrapper(run_with_joint)(setup, ccs);
    EXPECT_TRUE(s.IsOK()) << s.Str();
    return c;
  };

  for (size_t i = 0; i < 5000; i++) {
    auto setup = genInitialChanges();
    CCS ccs = genConfChanges();
    auto c1 = f1(setup, ccs);
    auto c2 = f2(setup, ccs);
    EXPECT_EQ(c1.GetProgressTracker().GetConfig(), c2.GetProgressTracker().GetConfig());
    EXPECT_TRUE(isEqual(c1.GetProgressTracker().GetProgressMap(), c2.GetProgressTracker().GetProgressMap()));
    EXPECT_EQ(c1.GetProgressTracker().Votes(), c2.GetProgressTracker().Votes());
  }
}
