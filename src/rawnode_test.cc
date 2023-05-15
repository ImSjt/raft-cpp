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

#include "gtest/gtest.h"
#include "raft_test_util.h"
#include "rawnode.h"
#include "util.h"
#include "raftpb/confstate.h"

bool confstateEqual(const raftpb::ConfState& cs1, const raftpb::ConfState& cs2) {
  auto c_cs1 = cs1;
  auto c_cs2 = cs2;
  for (auto cs : std::vector<raftpb::ConfState*>{&c_cs1, &c_cs2}) {
    std::sort(cs->mutable_voters()->begin(), cs->mutable_voters()->end());
    std::sort(cs->mutable_learners()->begin(), cs->mutable_learners()->end());
    std::sort(cs->mutable_voters_outgoing()->begin(),
              cs->mutable_voters_outgoing()->end());
    std::sort(cs->mutable_learners_next()->begin(),
              cs->mutable_learners_next()->end());
  }
  auto equal =
      [](const ::google::protobuf::RepeatedField< ::google::protobuf::uint64>& v1,
         const ::google::protobuf::RepeatedField< ::google::protobuf::uint64>& v2) {
      return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
    };

  return equal(c_cs1.voters(), c_cs2.voters()) &&
         equal(c_cs1.learners(), c_cs2.learners()) &&
         equal(c_cs1.voters_outgoing(), c_cs2.voters_outgoing()) &&
         equal(c_cs1.learners_next(), c_cs2.learners_next()) &&
         c_cs1.auto_leave() == c_cs2.auto_leave();
}

// TestRawNodeStep ensures that RawNode.Step ignore local message.
TEST(RawNode, Step) {
  // raftpb::MessageType
  for (int i = raftpb::MessageType_MIN; i <= raftpb::MessageType_MAX; i++) {
    auto s = std::make_shared<craft::MemoryStorage>();
    raftpb::HardState hs;
    hs.set_term(1);
    hs.set_commit(1);
    s->SetHardState(hs);
    s->Append({NEW_ENT().Term(1).Index(1)()});
    auto snap = std::make_shared<raftpb::Snapshot>();
    snap->mutable_metadata()->set_index(1);
    snap->mutable_metadata()->set_term(1);
    snap->mutable_metadata()->mutable_conf_state()->add_voters(1);
    s->ApplySnapshot(snap);
    // Append an empty entry to make sure the non-local messages (like
    // vote requests) are ignored and don't trigger assertions.
    auto raw_node = craft::RawNode::New(newTestConfig(1, 10, 1, s));
    auto msgt = static_cast<raftpb::MessageType>(i);
    auto status = raw_node->Step(NEW_MSG().Type(msgt)());
    // LocalMsg should be ignored.
    if (craft::Util::IsLocalMsg(msgt)) {
      ASSERT_STREQ(status.Str(), craft::kErrStepLocalMsg);
    }
  }
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange tests the configuration change mechanism. Each
// test case sends a configuration change which is either simple or joint, verifies
// that it applies and that the resulting ConfState matches expectations, and for
// joint configurations makes sure that they are exited successfully.
TEST(RawNode, ProposeAndConfChange) {
  struct Test {
    craft::ConfChangeI cc;
    raftpb::ConfState exp;
    std::shared_ptr<raftpb::ConfState> exp2;
  };
  raftpb::ConfChange ccv1_test;
  ccv1_test.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
  ccv1_test.set_node_id(2);
  std::vector<Test> tests = {
    // V1 config change.
    {
      ccv1_test,
      *NEW_CONF_STATE().Voters({1, 2})(),
      nullptr,
    },
		// Proposing the same as a V2 change works just the same, without entering
		// a joint config.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddNode, 2)(),
      *NEW_CONF_STATE().Voters({1, 2})(),
      nullptr,
    },
    // Ditto if we add it as a learner instead.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 2)(),
      *NEW_CONF_STATE().Voters({1}).Learners({2})(),
      nullptr,
    },
    // We can ask explicitly for joint consensus if we want it.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 2)
        .Transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointExplicit)(),
      *NEW_CONF_STATE().Voters({1}).VotersOutgoing({1}).Learners({2})(),
      NEW_CONF_STATE().Voters({1}).Learners({2})(),
    },
    // Ditto, but with implicit transition (the harness checks this).
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 2)
        .Transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointImplicit)(),
      *NEW_CONF_STATE().Voters({1}).VotersOutgoing({1}).Learners({2}).AutoLeave(true)(),
      NEW_CONF_STATE().Voters({1}).Learners({2})(),
    },
		// Add a new node and demote n1. This exercises the interesting case in
		// which we really need joint config changes and also need LearnersNext.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddNode, 2)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 1)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 3)(),
      *NEW_CONF_STATE().Voters({2}).VotersOutgoing({1}).Learners({3}).LearnersNext({1}).AutoLeave(true)(),
      NEW_CONF_STATE().Voters({2}).Learners({1, 3})(),
    },
    // Ditto explicit.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddNode, 2)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 1)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 3)
        .Transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointExplicit)(),
      *NEW_CONF_STATE().Voters({2}).VotersOutgoing({1}).Learners({3}).LearnersNext({1})(),
      NEW_CONF_STATE().Voters({2}).Learners({1, 3})(),
    },
    // Ditto implicit.
    {
      *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddNode, 2)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 1)
        .AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 3)
        .Transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointImplicit)(),
      *NEW_CONF_STATE().Voters({2}).VotersOutgoing({1}).Learners({3}).LearnersNext({1}).AutoLeave(true)(),
      NEW_CONF_STATE().Voters({2}).Learners({1, 3})(),
    }
  };
  for (auto& tt : tests) {
    auto s = newTestMemoryStorage({withPeers({1})});
    auto raw_node = craft::RawNode::New(newTestConfig(1, 10, 1, s));

    raw_node->Campaign();
    bool proposed = false;
    uint64_t last_index;
    std::string ccdata;
    // Propose the ConfChange, wait until it applies, save the resulting
    // ConfState.
    std::shared_ptr<raftpb::ConfState> cs;
    while (!cs) {
      auto rd = raw_node->GetReady();
      s->Append(rd.entries);
      for (auto& ent : rd.committed_entries) {
        craft::ConfChangeI cc;
        if (ent->type() == raftpb::EntryType::EntryConfChange) {
          raftpb::ConfChange ccc;
          ASSERT_TRUE(ccc.ParseFromString(ent->data()));
          cc.SetConfChange(std::move(ccc));
        } else if (ent->type() == raftpb::EntryType::EntryConfChangeV2) {
          raftpb::ConfChangeV2 ccc;
          ASSERT_TRUE(ccc.ParseFromString(ent->data()));
          cc.SetConfChange(std::move(ccc));
        }
        if (!cc.IsNull()) {
          cs = std::make_shared<raftpb::ConfState>(raw_node->ApplyConfChange(cc));
        }
      }
      raw_node->Advance(rd);
      // Once we are the leader, propose a command and a ConfChange.
      if (!proposed && rd.soft_state->lead == raw_node->GetRaft()->ID()) {
        auto s1 = raw_node->Propose("somedata");
        ASSERT_TRUE(s1.IsOK());
        auto [ccv1, ok] = tt.cc.AsV1();
        if (ok) {
          ccdata = ccv1.SerializeAsString();
          raw_node->ProposeConfChange(ccv1);
        } else {
          auto ccv2 = tt.cc.AsV2();
          ccdata = ccv2.SerializeAsString();
          raw_node->ProposeConfChange(ccv2);
        }
        proposed = true;
      }
    }

    // Check that the last index is exactly the conf change we put in,
    // down to the bits. Note that this comes from the Storage, which
    // will not reflect any unstable entries that we'll only be presented
    // with in the next Ready.
    craft::Status s1;
    std::tie(last_index, s1) = s->LastIndex();
    ASSERT_EQ(s1.IsOK(), true);

    auto [entries, s2] = s->Entries(last_index-1, last_index+1, craft::Raft::kNoLimit);
    ASSERT_EQ(s2.IsOK(), true);
    ASSERT_EQ(entries.size(), 2);
    ASSERT_EQ(entries[0]->data(), "somedata");

    auto typ = raftpb::EntryType::EntryConfChange;
    auto [ccv1, ok] = tt.cc.AsV1();
    if (!ok) {
      typ = raftpb::EntryType::EntryConfChangeV2;
    }
    ASSERT_EQ(entries[1]->type(), typ);
    ASSERT_EQ(entries[1]->data(), ccdata);

    ASSERT_TRUE(confstateEqual(*cs, tt.exp));

    uint64_t maybe_plus_one = 0;
    auto [auto_leave, ok2] = craft::EnterJoint(tt.cc.AsV2());
    if (ok2 && auto_leave) {
      // If this is an auto-leaving joint conf change, it will have
      // appended the entry that auto-leaves, so add one to the last
      // index that forms the basis of our expectations on
      // pendingConfIndex. (Recall that lastIndex was taken from stable
      // storage, but this auto-leaving entry isn't on stable storage
      // yet).
      maybe_plus_one = 1;
    }
    auto exp = last_index + maybe_plus_one;
    auto act = raw_node->GetRaft()->PendingConfIndex();
    ASSERT_EQ(exp, act);

    // Move the RawNode along. If the ConfChange was simple, nothing else
    // should happen. Otherwise, we're in a joint state, which is either
    // left automatically or not. If not, we add the proposal that leaves
    // it manually.
    auto rd = raw_node->GetReady();
    std::string context;
    if (!tt.exp.auto_leave()) {
      ASSERT_TRUE(rd.entries.empty());
      if (tt.exp2 == nullptr) {
        continue;
      }
      context = "manual";
      raftpb::ConfChangeV2 ccv2;
      ccv2.set_context(context);
      auto status = raw_node->ProposeConfChange(craft::ConfChangeI(std::move(ccv2)));
      ASSERT_TRUE(status.IsOK());
      rd = raw_node->GetReady();
    }

    // Check that the right ConfChange comes out.
    ASSERT_EQ(rd.entries.size(), 1);
    ASSERT_EQ(rd.entries[0]->type(), raftpb::EntryType::EntryConfChangeV2);

    raftpb::ConfChangeV2 cc;
    ASSERT_TRUE(cc.ParseFromString(rd.entries[0]->data()));
    ASSERT_EQ(cc.context(), context);
    // Lie and pretend the ConfChange applied. It won't do so because now
    // we require the joint quorum and we're only running one node.
    *cs = raw_node->ApplyConfChange(cc);
    ASSERT_TRUE(tt.exp2 != nullptr);
    ASSERT_TRUE(confstateEqual(*cs, *tt.exp2));
  }
}

// TestRawNodeJointAutoLeave tests the configuration change auto leave even leader
// lost leadership.
TEST(RawNode, JointAutoLeave) {
  auto test_cc = *NEW_CONF_CHANGE().AddConf(raftpb::ConfChangeType::ConfChangeAddLearnerNode, 2)
    .Transition(raftpb::ConfChangeTransition::ConfChangeTransitionJointImplicit)();
  auto exp_cs = *NEW_CONF_STATE().Voters({1}).VotersOutgoing({1}).Learners({2}).AutoLeave(true)();
  auto exp2_cs = *NEW_CONF_STATE().Voters({1}).Learners({2})();

  auto s = newTestMemoryStorage({withPeers({1})});
  auto raw_node = craft::RawNode::New(newTestConfig(1, 10, 1, s));

  raw_node->Campaign();
  bool proposed = false;
  uint64_t last_index;
  std::string ccdata;
  // Propose the ConfChange, wait until it applies, save the resulting
  // ConfState.
  std::shared_ptr<raftpb::ConfState> cs;
  while (!cs) {
    auto rd = raw_node->GetReady();
    s->Append(rd.entries);
    for (auto& ent : rd.committed_entries) {
      craft::ConfChangeI cc;
      if (ent->type() == raftpb::EntryType::EntryConfChangeV2) {
        raftpb::ConfChangeV2 ccc;
        ASSERT_TRUE(ccc.ParseFromString(ent->data()));
        cc.SetConfChange(std::move(ccc));
      }
      if (!cc.IsNull()) {
        // Force it step down.
        raw_node->Step(NEW_MSG().Type(raftpb::MessageType::MsgHeartbeatResp).From(1).Term(raw_node->GetRaft()->Term() + 1)());
        cs = std::make_shared<raftpb::ConfState>(raw_node->ApplyConfChange(cc));
      }
    }
    raw_node->Advance(rd);
    // Once we are the leader, propose a command and a ConfChange.
    if (!proposed && rd.soft_state->lead == raw_node->GetRaft()->ID()) {
      auto s1 = raw_node->Propose("somedata");
      ASSERT_TRUE(s1.IsOK());
      ccdata = test_cc.SerializeAsString();
      ASSERT_TRUE(s1.IsOK());
      raw_node->ProposeConfChange(test_cc);
      proposed = true;
    }
  }

  // Check that the last index is exactly the conf change we put in,
  // down to the bits. Note that this comes from the Storage, which
  // will not reflect any unstable entries that we'll only be presented
  // with in the next Ready.
  craft::Status s1;
  std::tie(last_index, s1) = s->LastIndex();
  ASSERT_EQ(s1.IsOK(), true);

  auto [entries, s2] = s->Entries(last_index-1, last_index+1, craft::Raft::kNoLimit);
  ASSERT_EQ(s2.IsOK(), true);
  ASSERT_EQ(entries.size(), 2);
  ASSERT_EQ(entries[0]->data(), "somedata");
  ASSERT_EQ(entries[1]->type(), raftpb::EntryType::EntryConfChangeV2);
  ASSERT_EQ(entries[1]->data(), ccdata);
  ASSERT_TRUE(confstateEqual(*cs, exp_cs));
  ASSERT_EQ(raw_node->GetRaft()->PendingConfIndex(), 0);

  // Move the RawNode along. It should not leave joint because it's follower.
  auto rd = raw_node->ReadyWithoutAccept();
  // Check that the right ConfChange comes out.
  ASSERT_EQ(rd.entries.size(), 0);

  // Make it leader again. It should leave joint automatically after moving apply index.
  raw_node->Campaign();
  rd = raw_node->GetReady();
  s->Append(rd.entries);
  raw_node->Advance(rd);
  rd = raw_node->GetReady();
  s->Append(rd.entries);

  // Check that the right ConfChange comes out.
  ASSERT_EQ(rd.entries.size(), 1);
  ASSERT_EQ(rd.entries[0]->type(), raftpb::EntryType::EntryConfChangeV2);

  raftpb::ConfChangeV2 cc;
  ASSERT_TRUE(cc.ParseFromString(rd.entries[0]->data()));
  ASSERT_EQ(cc.context().empty(), true);
  // Lie and pretend the ConfChange applied. It won't do so because now
  // we require the joint quorum and we're only running one node.
  *cs = raw_node->ApplyConfChange(cc);
  ASSERT_TRUE(confstateEqual(*cs, exp2_cs));
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
TEST(RawNode, RawNodeProposeAddDuplicateNode) {
  auto s = newTestMemoryStorage({withPeers({1})});
  auto raw_node = craft::RawNode::New(newTestConfig(1, 10, 1, s));
  auto rd = raw_node->GetReady();
  s->Append(rd.entries);
  raw_node->Advance(rd);

  raw_node->Campaign();
  while (1) {
    rd = raw_node->GetReady();
    s->Append(rd.entries);
    if (rd.soft_state->lead == raw_node->GetRaft()->ID()) {
      raw_node->Advance(rd);
      break;
    }
    raw_node->Advance(rd);
  }

  auto propose_confchange_and_apply = [&raw_node, &rd, &s](raftpb::ConfChange cc) {
    raw_node->ProposeConfChange(craft::ConfChangeI(cc));
    rd = raw_node->GetReady();
    s->Append(rd.entries);
    for (auto entry : rd.committed_entries) {
      if (entry->type() == raftpb::EntryType::EntryConfChange) {
        raftpb::ConfChange cc;
        cc.ParseFromString(entry->data());
        raw_node->ApplyConfChange(craft::ConfChangeI(cc));
      }
    }
    raw_node->Advance(rd);
  };

  raftpb::ConfChange cc1;
  cc1.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
  cc1.set_node_id(1);
  auto ccdata1 = cc1.SerializeAsString();
  propose_confchange_and_apply(cc1);

  // try to add the same node again
  propose_confchange_and_apply(cc1);

	// the new node join should be ok
  raftpb::ConfChange cc2;
  cc2.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
  cc2.set_node_id(2);
  auto ccdata2 = cc2.SerializeAsString();
  propose_confchange_and_apply(cc2);

  auto [last_index, s1] = s->LastIndex();
  ASSERT_TRUE(s1.IsOK());

	// the last three entries should be: ConfChange cc1, cc1, cc2
  auto [entries, s2] = s->Entries(last_index-2, last_index+1, craft::Raft::kNoLimit);
  ASSERT_EQ(entries.size(), 3);
  ASSERT_EQ(entries[0]->data(), ccdata1);
  ASSERT_EQ(entries[2]->data(), ccdata2);
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
TEST(RawNode, ReadIndex) {

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}