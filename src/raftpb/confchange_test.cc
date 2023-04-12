// Copyright 2023 JT
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
#include <iostream>

#include "gtest/gtest.h"
#include "raftpb/confchange.h"

TEST(ConfChange, IsNull) {
  struct Test {
    craft::ConfChangeI cc;
    bool w;
  };
  std::vector<Test> tests = {
    {craft::ConfChangeI(), true},
    {craft::ConfChangeI(raftpb::ConfChange()), false},
    {craft::ConfChangeI(raftpb::ConfChangeV2()), false},
  };
  for (auto& tt : tests) {
    ASSERT_EQ(tt.cc.IsNull(), tt.w);
  }
}

TEST(ConfChange, AsV2) {
  {
    raftpb::ConfChange cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(1);
    cc.set_context("hello");
    craft::ConfChangeI cci(cc);
    auto cc_v2 = cci.AsV2();
    ASSERT_EQ(cc_v2.changes_size(), 1);
    ASSERT_EQ(cc_v2.changes(0).type(), cc.type());
    ASSERT_EQ(cc_v2.changes(0).node_id(), cc.node_id());
    ASSERT_EQ(cc_v2.context(), cc.context());
  }

  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionAuto);
    cc.set_context("hello");
    auto cs = cc.add_changes();
    cs->set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    cs->set_node_id(2);
    craft::ConfChangeI cci(cc);
    auto cc_v2 = cci.AsV2();
    ASSERT_EQ(cc_v2.transition(), cc.transition());
    ASSERT_EQ(cc_v2.context(), cc.context());
    ASSERT_EQ(cc_v2.changes_size(), cc.changes_size());
    for (size_t i = 0; i < cc_v2.changes_size(); i++) {
      ASSERT_EQ(cc_v2.changes(i).type(), cc.changes(i).type());
      ASSERT_EQ(cc_v2.changes(i).node_id(), cc.changes(i).node_id());
    }
  }
}

TEST(ConfChange, AsV1) {
  {
    raftpb::ConfChange cc;
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(1);
    cc.set_context("hello");
    cc.set_id(123);
    craft::ConfChangeI cci(cc);
    auto [cc_v1, ok] = cci.AsV1();
    ASSERT_TRUE(ok);
    ASSERT_EQ(cc_v1.type(), cc.type());
    ASSERT_EQ(cc_v1.node_id(), cc.node_id());
    ASSERT_EQ(cc_v1.context(), cc.context());
    ASSERT_EQ(cc_v1.id(), cc.id());
  }

  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionAuto);
    cc.set_context("hello");
    auto cs = cc.add_changes();
    cs->set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    cs->set_node_id(2);
    craft::ConfChangeI cci(cc);
    auto [cc_v1, ok] = cci.AsV1();
    ASSERT_FALSE(ok);
  }
}

TEST(ConfChange, EnterJoint) {
  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionAuto);
    ASSERT_EQ(craft::EnterJoint(cc), std::make_tuple(false, false));
  }

  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionAuto);
    cc.add_changes();
    cc.add_changes();
    ASSERT_EQ(craft::EnterJoint(cc), std::make_tuple(true, true));
  }

  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointImplicit);
    cc.add_changes();
    cc.add_changes();
    ASSERT_EQ(craft::EnterJoint(cc), std::make_tuple(true, true));
  }

  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointExplicit);
    cc.add_changes();
    cc.add_changes();
    ASSERT_EQ(craft::EnterJoint(cc), std::make_tuple(false, true));
  }
}

TEST(ConfChange, LeaveJoint) {
  {
    raftpb::ConfChangeV2 cc;
    ASSERT_EQ(craft::LeaveJoint(cc), true);
  }
  {
    raftpb::ConfChangeV2 cc;
    cc.add_changes();
    ASSERT_EQ(craft::LeaveJoint(cc), false);
  }
  {
    raftpb::ConfChangeV2 cc;
    cc.set_context("123");
    ASSERT_EQ(craft::LeaveJoint(cc), true);
  }
  {
    raftpb::ConfChangeV2 cc;
    cc.set_transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointExplicit);
    ASSERT_EQ(craft::LeaveJoint(cc), false);
  }
}

TEST(ConfChange, ConfChangesFromString) {
  std::string s = "v1 r2 u3 l4";

  auto [ccs, status] = craft::ConfChangesFromString(s);
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(ccs.size(), 4);
  
  ASSERT_EQ(ccs[0].type(), raftpb::ConfChangeType::ConfChangeAddNode);
  ASSERT_EQ(ccs[0].node_id(), 1);

  ASSERT_EQ(ccs[1].type(), raftpb::ConfChangeType::ConfChangeRemoveNode);
  ASSERT_EQ(ccs[1].node_id(), 2);

  ASSERT_EQ(ccs[2].type(), raftpb::ConfChangeType::ConfChangeUpdateNode);
  ASSERT_EQ(ccs[2].node_id(), 3);

  ASSERT_EQ(ccs[3].type(), raftpb::ConfChangeType::ConfChangeAddLearnerNode);
  ASSERT_EQ(ccs[3].node_id(), 4);
}

TEST(ConfChange, ConfChangesToString) {
  std::vector<raftpb::ConfChangeSingle> ccs;
  raftpb::ConfChangeSingle cc;
  {
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    cc.set_node_id(1);
    ccs.emplace_back(cc);
  }
  {
    cc.set_type(raftpb::ConfChangeType::ConfChangeRemoveNode);
    cc.set_node_id(2);
    ccs.emplace_back(cc);
  }
  {
    cc.set_type(raftpb::ConfChangeType::ConfChangeUpdateNode);
    cc.set_node_id(3);
    ccs.emplace_back(cc);
  }
  {
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    cc.set_node_id(4);
    ccs.emplace_back(cc);
  }
  std::string w = "v1 r2 u3 l4";
  ASSERT_EQ(craft::ConfChangesToString(ccs), w);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}