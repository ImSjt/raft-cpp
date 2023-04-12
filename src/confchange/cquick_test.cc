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

#include "gtest/gtest.h"
#include "confchange/confchange.h"

TEST(ConfChange, Quick) {
  using CCS = std::vector<raftpb::ConfChangeSingle>;
  auto run_with_joint = [](craft::Changer& c,
                           const CCS& ccs) {
    auto [cfg, prs, s1] = c.EnterJoint(false, ccs);
    if (!s1.IsOK()) {
      return s1;
    }
		// Also do this with autoLeave on, just to check that we'd get the same
		// result.
    auto [cfg2a, prs2a, s2] = c.EnterJoint(true, ccs);
    if (!s2.IsOK()) {
      return s2;
    }
    cfg2a.auto_leave_ = false;
    EXPECT_EQ(cfg, cfg2a);
    EXPECT_EQ(prs, prs2a);

    c.GetProgressTracker().SetConfig(cfg);
    c.GetProgressTracker().SetProgressMap(prs);
    auto [cfg2b, prs2b, s3] = c.LeaveJoint();
    if (!s3.IsOK()) {
      return s3;
    }
    // Reset back to the main branch with autoLeave=false.
    c.GetProgressTracker().SetConfig(cfg);
    c.GetProgressTracker().SetProgressMap(prs);
    auto [cfg2c, prs2c, s4] = c.LeaveJoint();
    if (!s4.IsOK()) {
      return s4;
    }
    EXPECT_EQ(cfg2b, cfg2c);
    EXPECT_EQ(prs2b, prs2c);

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
      craft::Changer c(tr, 10);
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
    EXPECT_TRUE(s.IsOK());
    return c;
  };

  auto f2 = [&wrapper, &run_with_joint](const CCS& setup, const CCS& ccs) {
    auto [c, s] = wrapper(run_with_joint)(setup, ccs);
    EXPECT_TRUE(s.IsOK()) << s.Str();
    return c;
  };

  CCS setup;
  CCS ccs;
  f1(setup, ccs);
  f2(setup, ccs);
}
