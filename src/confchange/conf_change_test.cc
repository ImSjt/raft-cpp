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
#include <sstream>

#include "gtest/gtest.h"
#include "confchange/confchange.h"
#include "raftpb/confchange.h"

static std::string progreMapString(const craft::ProgressMap& m) {
  std::stringstream ss;
  for (auto& p : m) {
    ss << "{" << p.first << ": " << p.second->String() << "}";
  }
  return ss.str();
}

TEST(ConfChange, JointAutoLeave) {
  craft::Changer chg(craft::ProgressTracker(10), 0);

  {
    auto [ccs, s] = craft::ConfChangesFromString(std::string("v1"));
    ASSERT_TRUE(s.IsOK());
    auto [cfg, prs, s2] = chg.Simple(ccs);
    ASSERT_TRUE(s2.IsOK());
    std::cout << cfg.String() << std::endl;
    std::cout << progreMapString(prs) << std::endl;
    chg.GetProgressTracker().SetConfig(cfg);
    chg.GetProgressTracker().SetProgressMap(prs);
    chg.SetLastIndex(chg.LastIndex() + 1);
  }

  {
    auto [ccs, s] = craft::ConfChangesFromString(std::string("v2 v3"));
    ASSERT_TRUE(s.IsOK());
    auto [cfg, prs, s2] = chg.EnterJoint(true, ccs);
    ASSERT_TRUE(s2.IsOK());
    std::cout << cfg.String() << std::endl;
    std::cout << progreMapString(prs) << std::endl;
    chg.GetProgressTracker().SetConfig(cfg);
    chg.GetProgressTracker().SetProgressMap(prs);
    chg.SetLastIndex(chg.LastIndex() + 1);
  }

  // Can't enter-joint twice, even if autoleave changes.
  {
    std::vector<raftpb::ConfChangeSingle> ccs;
    auto [cfg, prs, s] = chg.EnterJoint(false, ccs);
    std::cout << s.Str() << std::endl;
  }

  {
    auto [cfg, prs, s] = chg.LeaveJoint();
    ASSERT_TRUE(s.IsOK());
    std::cout << cfg.String() << std::endl;
    std::cout << progreMapString(prs) << std::endl;
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}