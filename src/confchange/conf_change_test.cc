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
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
      {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
       "{1: StateProbe match=0 next=0}"},
      {"enter-joint", "v2 v3", true, craft::Status::OK(),
       "voters=(1 2 3)&&(1) autoleave",
       "{1: StateProbe match=0 next=0}{2: StateProbe match=0 "
       "next=1}{3: StateProbe match=0 next=1}"},
      {"enter-joint", "", false,
       craft::Status::Error("config is already joint"), "", ""},
      {"leave-joint", "", false, craft::Status::OK(), "voters=(1 2 3)",
       "{1: StateProbe match=0 next=0}{2: StateProbe match=0 "
       "next=1}{3: StateProbe match=0 next=1}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, JointIdempotency) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=0}"},
    {"enter-joint", "r1 r2 r9 v2 v3 v4 v2 v3 v4 l2 l2 r4 r4 l1 l1", false,
      craft::Status::OK(), "voters=(3)&&(1) learners=(2) learners_next=(1)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1 "
      "learner}{3: StateProbe match=0 next=1}"},
    {"leave-joint", "", false, craft::Status::OK(),
      "voters=(3) learners=(1 2)",
      "{1: StateProbe match=0 next=0 learner}{2: StateProbe match=0 next=1 "
      "learner}{3: StateProbe match=0 next=1}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, JointLearnersNext) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=0}"},
    {"enter-joint", "v2 l1", false, craft::Status::OK(),
      "voters=(2)&&(1) learners_next=(1)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}"},
    {"leave-joint", "", false, craft::Status::OK(), "voters=(2) learners=(1)",
      "{1: StateProbe match=0 next=0 learner}{2: StateProbe match=0 next=1}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, JointSafety) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"leave-joint", "", false,
      craft::Status::Error("can't leave a non-joint config"), "", ""},
    {"enter-joint", "", false,
      craft::Status::Error("can't make a zero-voter config joint"), "", ""},
    {"enter-joint", "v1", false,
      craft::Status::Error("can't make a zero-voter config joint"), "", ""},
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=3}"},
    {"leave-joint", "", false,
      craft::Status::Error("can't leave a non-joint config"), "", ""},
    {"enter-joint", "", false, craft::Status::OK(), "voters=(1)&&(1)",
      "{1: StateProbe match=0 next=3}"},
    {"enter-joint", "", false,
      craft::Status::Error("config is already joint"), "", ""},
    {"leave-joint", "", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=3}"},
    {"leave-joint", "", false,
      craft::Status::Error("can't leave a non-joint config"), "", ""},
    {"enter-joint", "r1 v2 v3 l4", false, craft::Status::OK(),
      "voters=(2 3)&&(1) learners=(4)",
      "{1: StateProbe match=0 next=3}{2: StateProbe match=0 next=9}{3: "
      "StateProbe match=0 next=9}{4: StateProbe match=0 next=9 learner}"},
    {"enter-joint", "", false,
      craft::Status::Error("config is already joint"), "", ""},
    {"enter-joint", "v12", false,
      craft::Status::Error("config is already joint"), "", ""},
    {"simple", "l15", false,
      craft::Status::Error("can't apply simple config change in joint config"),
      "", ""},
    {"leave-joint", "", false, craft::Status::OK(),
      "voters=(2 3) learners=(4)",
      "{2: StateProbe match=0 next=9}{3: StateProbe match=0 next=9}{4: "
      "StateProbe match=0 next=9 learner}"},
    {"simple", "l9", false, craft::Status::OK(),
      "voters=(2 3) learners=(4 9)",
      "{2: StateProbe match=0 next=9}{3: StateProbe match=0 next=9}{4: "
      "StateProbe match=0 next=9 learner}{9: StateProbe match=0 next=14 "
      "learner}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, SimpleIdempotency) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=0}"},
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=0}"},
    {"simple", "v2", false, craft::Status::OK(), "voters=(1 2)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=2}"},
    {"simple", "l1", false, craft::Status::OK(), "voters=(2) learners=(1)",
      "{1: StateProbe match=0 next=0 learner}{2: StateProbe match=0 next=2}"},
    {"simple", "l1", false, craft::Status::OK(), "voters=(2) learners=(1)",
      "{1: StateProbe match=0 next=0 learner}{2: StateProbe match=0 next=2}"},
    {"simple", "r1", false, craft::Status::OK(), "voters=(2)",
      "{2: StateProbe match=0 next=2}"},
    {"simple", "r1", false, craft::Status::OK(), "voters=(2)",
      "{2: StateProbe match=0 next=2}"},
    {"simple", "v3", false, craft::Status::OK(), "voters=(2 3)",
      "{2: StateProbe match=0 next=2}{3: StateProbe match=0 next=7}"},
    {"simple", "r3", false, craft::Status::OK(), "voters=(2)",
      "{2: StateProbe match=0 next=2}"},
    {"simple", "r3", false, craft::Status::OK(), "voters=(2)",
      "{2: StateProbe match=0 next=2}"},
    {"simple", "r4", false, craft::Status::OK(), "voters=(2)",
      "{2: StateProbe match=0 next=2}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, SimplePromoteDemote) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
      "{1: StateProbe match=0 next=0}"},
    {"simple", "v2", false, craft::Status::OK(), "voters=(1 2)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}"},
    {"simple", "v3", false, craft::Status::OK(), "voters=(1 2 3)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}{3: "
      "StateProbe match=0 next=2}"},
    {"simple", "l1 v1", false, craft::Status::OK(), "voters=(1 2 3)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}{3: "
      "StateProbe match=0 next=2}"},
    {"simple", "l2", false, craft::Status::OK(), "voters=(1 3) learners=(2)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1 "
      "learner}{3: StateProbe match=0 next=2}"},
    {"simple", "v2 l2", false, craft::Status::OK(),
      "voters=(1 3) learners=(2)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1 "
      "learner}{3: StateProbe match=0 next=2}"},
    {"simple", "v2", false, craft::Status::OK(), "voters=(1 2 3)",
      "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}{3: "
      "StateProbe match=0 next=2}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, SimpleSafety) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
      {"simple", "l1", false, craft::Status::Error("removed all voters"), "",
       ""},
      {"simple", "v1", false, craft::Status::OK(), "voters=(1)",
       "{1: StateProbe match=0 next=1}"},
      {"simple", "v2 l3", false, craft::Status::OK(),
       "voters=(1 2) learners=(3)",
       "{1: StateProbe match=0 next=1}{2: StateProbe match=0 next=2}{3: "
       "StateProbe match=0 next=2 learner}"},
      {"simple", "r1 v5", false,
       craft::Status::Error(
           "more than one voter changed without entering joint config"),
       "", ""},
      {"simple", "r1 r2", false, craft::Status::Error("removed all voters"), "",
       ""},
      {"simple", "v3 v4", false,
       craft::Status::Error(
           "more than one voter changed without entering joint config"),
       "", ""},
      {"simple", "l1 v5", false,
       craft::Status::Error(
           "more than one voter changed without entering joint config"),
       "", ""},
      {"simple", "l1 l2", false, craft::Status::Error("removed all voters"), "",
       ""},
      {"simple", "l2 l3 l4 l5", false, craft::Status::OK(),
       "voters=(1) learners=(2 3 4 5)",
       "{1: StateProbe match=0 next=1}{2: StateProbe match=0 next=2 "
       "learner}{3: StateProbe match=0 next=2 learner}{4: StateProbe match=0 "
       "next=8 learner}{5: StateProbe match=0 next=8 learner}"},
      {"simple", "r1", false, craft::Status::Error("removed all voters"), "",
       ""},
      {"simple", "r2 r3 r4 r5", false, craft::Status::OK(), "voters=(1)",
       "{1: StateProbe match=0 next=1}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, Update) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1", false, craft::Status::OK(), "voters=(1)", "{1: StateProbe match=0 next=0}"},
    {"simple", "v2 u1", false, craft::Status::OK(), "voters=(1 2)", "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}"},
    {"simple", "u1 u2 u3 u1 u2 u3", false, craft::Status::OK(), "voters=(1 2)", "{1: StateProbe match=0 next=0}{2: StateProbe match=0 next=1}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

TEST(ConfChange, Zero) {
  struct Test {
    std::string op;
    std::string ccs;
    bool auto_leave;

    craft::Status ws;
    std::string wcfg;
    std::string wprs;
  };
  std::vector<Test> tests = {
    {"simple", "v1 r0 v0 l0", false, craft::Status::OK(), "voters=(1)", "{1: StateProbe match=0 next=0}"},
  };

  craft::Changer chg(craft::ProgressTracker(10), 0);
  for (auto& tt : tests) {
    std::vector<raftpb::ConfChangeSingle> ccs;
    if (!tt.ccs.empty()) {
      craft::Status s;
      std::tie(ccs, s) = craft::ConfChangesFromString(tt.ccs);
      ASSERT_TRUE(s.IsOK());
    }

    craft::ProgressTracker::Config cfg;
    craft::ProgressMap prs;
    craft::Status s;
    if (tt.op == "simple") {
      std::tie(cfg, prs, s) = chg.Simple(ccs);
    } else if (tt.op == "enter-joint") {
      std::tie(cfg, prs, s) = chg.EnterJoint(tt.auto_leave, ccs);
    } else if (tt.op == "leave-joint") {
      std::tie(cfg, prs, s) = chg.LeaveJoint();
    } else {
      ASSERT_TRUE(false);
    }
    ASSERT_EQ(s.IsOK(), tt.ws.IsOK());
    if (!s.IsOK()) {
      ASSERT_STREQ(s.Str(), tt.ws.Str());
    } else {
      ASSERT_EQ(cfg.String(), tt.wcfg);
      ASSERT_EQ(progreMapString(prs), tt.wprs);
      chg.GetProgressTracker().SetConfig(std::move(cfg));
      chg.GetProgressTracker().SetProgressMap(std::move(prs));
    }
    chg.SetLastIndex(chg.LastIndex() + 1);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}