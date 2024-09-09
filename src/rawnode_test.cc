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
#include "src/raft_test_util.h"
#include "src/rawnode.h"
#include "src/util.h"
#include "src/raftpb/confstate.h"

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
    auto s = std::make_shared<craft::MemoryStorage>(std::make_shared<craft::ConsoleLogger>());
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
      s->Append(rd->entries);
      for (auto& ent : rd->committed_entries) {
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
      raw_node->Advance();
      // Once we are the leader, propose a command and a ConfChange.
      if (!proposed && rd->soft_state->lead == raw_node->GetRaft()->ID()) {
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
    ASSERT_EQ(entries.size(), static_cast<size_t>(2));
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
    auto [auto_leave, ok2] = craft::EnterJoint(std::make_shared<craft::ConsoleLogger>(), tt.cc.AsV2());
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
      ASSERT_TRUE(rd->entries.empty());
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
    ASSERT_EQ(rd->entries.size(), static_cast<size_t>(1));
    ASSERT_EQ(rd->entries[0]->type(), raftpb::EntryType::EntryConfChangeV2);

    raftpb::ConfChangeV2 cc;
    ASSERT_TRUE(cc.ParseFromString(rd->entries[0]->data()));
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
    s->Append(rd->entries);
    for (auto& ent : rd->committed_entries) {
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
    raw_node->Advance();
    // Once we are the leader, propose a command and a ConfChange.
    if (!proposed && rd->soft_state->lead == raw_node->GetRaft()->ID()) {
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
  ASSERT_EQ(entries.size(), static_cast<size_t>(2));
  ASSERT_EQ(entries[0]->data(), "somedata");
  ASSERT_EQ(entries[1]->type(), raftpb::EntryType::EntryConfChangeV2);
  ASSERT_EQ(entries[1]->data(), ccdata);
  ASSERT_TRUE(confstateEqual(*cs, exp_cs));
  ASSERT_EQ(raw_node->GetRaft()->PendingConfIndex(), static_cast<uint64_t>(0));

  // Move the RawNode along. It should not leave joint because it's follower.
  auto rd = raw_node->ReadyWithoutAccept();
  // Check that the right ConfChange comes out.
  ASSERT_EQ(rd->entries.size(), static_cast<size_t>(0));

  // Make it leader again. It should leave joint automatically after moving apply index.
  raw_node->Campaign();
  rd = raw_node->GetReady();
  s->Append(rd->entries);
  raw_node->Advance();
  rd = raw_node->GetReady();
  s->Append(rd->entries);

  // Check that the right ConfChange comes out.
  ASSERT_EQ(rd->entries.size(), static_cast<size_t>(1));
  ASSERT_EQ(rd->entries[0]->type(), raftpb::EntryType::EntryConfChangeV2);

  raftpb::ConfChangeV2 cc;
  ASSERT_TRUE(cc.ParseFromString(rd->entries[0]->data()));
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
  s->Append(rd->entries);
  raw_node->Advance();

  raw_node->Campaign();
  while (1) {
    rd = raw_node->GetReady();
    s->Append(rd->entries);
    if (rd->soft_state->lead == raw_node->GetRaft()->ID()) {
      raw_node->Advance();
      break;
    }
    raw_node->Advance();
  }

  auto propose_confchange_and_apply = [&raw_node, &rd, &s](raftpb::ConfChange cc) {
    raw_node->ProposeConfChange(craft::ConfChangeI(cc));
    rd = raw_node->GetReady();
    s->Append(rd->entries);
    for (auto entry : rd->committed_entries) {
      if (entry->type() == raftpb::EntryType::EntryConfChange) {
        raftpb::ConfChange cc;
        cc.ParseFromString(entry->data());
        raw_node->ApplyConfChange(craft::ConfChangeI(cc));
      }
    }
    raw_node->Advance();
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
  ASSERT_EQ(entries.size(), static_cast<size_t>(3));
  ASSERT_EQ(entries[0]->data(), ccdata1);
  ASSERT_EQ(entries[2]->data(), ccdata2);
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
TEST(RawNode, ReadIndex) {
  craft::MsgPtrs msgs;
  auto append_step = [&msgs](craft::MsgPtr m) {
    msgs.push_back(m);
    return craft::Status::OK();
  };
  std::deque<craft::ReadState> wrs = {
    {1, "somedata"},
  };

  auto s = newTestMemoryStorage({withPeers({1})});
  auto c = newTestConfig(1, 10, 1, s);
  auto rawnode = craft::RawNode::New(c);
  rawnode->GetRaft()->SetReadStates(wrs);
  // ensure the ReadStates can be read out
  bool has_ready = rawnode->HasReady();
  ASSERT_TRUE(has_ready);

  auto rd = rawnode->GetReady();
  ASSERT_EQ(rd->read_states, wrs);
  s->Append(rd->entries);
  rawnode->Advance();
  // ensure raft.readStates is reset after advance
  ASSERT_EQ(rawnode->GetRaft()->GetReadStates().size(), static_cast<size_t>(0));

  std::string wrequest_ctx = "somedata2";
  rawnode->Campaign();
  while (1) {
    rd = rawnode->GetReady();
    s->Append(rd->entries);

    if (rd->soft_state->lead == rawnode->GetRaft()->ID()) {
      rawnode->Advance();

      // Once we are the leader, issue a ReadIndex request
      rawnode->GetRaft()->SetStep(append_step);
      rawnode->ReadIndex(wrequest_ctx);
      break;
    }
    rawnode->Advance();
  }
  // ensure that MsgReadIndex message is sent to the underlying raft
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgReadIndex);
  ASSERT_EQ(msgs[0]->entries(0).data(), wrequest_ctx);
}

static void readyEqual(const craft::Ready& readya, const craft::Ready& readyb) {
  ASSERT_EQ(readya.soft_state, readyb.soft_state);

  ASSERT_EQ(readya.hard_state.term(), readyb.hard_state.term());
  ASSERT_EQ(readya.hard_state.vote(), readyb.hard_state.vote());
  ASSERT_EQ(readya.hard_state.commit(), readyb.hard_state.commit());

  ASSERT_EQ(readya.read_states, readyb.read_states);

  ASSERT_EQ(readya.entries.size(), readyb.entries.size());

  ASSERT_EQ(readya.committed_entries.size(), readyb.committed_entries.size());

  ASSERT_EQ(readya.messages.size(), readyb.messages.size());

  ASSERT_EQ(readya.must_sync, readyb.must_sync);
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. Note that RawNode
// requires the application to bootstrap the state, i.e. it does not accept peers
// and will not create faux configuration change entries.
TEST(RawNode, Start) {
  craft::Ready want;
  want.soft_state = craft::SoftState{1, craft::RaftStateType::kLeader};
  raftpb::HardState hs;
  hs.set_term(1);
  hs.set_commit(3);
  hs.set_vote(1);
  want.hard_state = hs;
  want.entries = {
    NEW_ENT().Term(1).Index(2)(),
    NEW_ENT().Term(1).Index(3).Data("foo")()
  };
  want.committed_entries = {
    NEW_ENT().Term(1).Index(2)(),
    NEW_ENT().Term(1).Index(3).Data("foo")()
  };
  want.must_sync = true;

  auto storage = std::make_shared<craft::MemoryStorage>(std::make_shared<craft::ConsoleLogger>());
  storage->GetEntry(0)->set_index(1);

	// TODO(tbg): this is a first prototype of what bootstrapping could look
	// like (without the annoying faux ConfChanges). We want to persist a
	// ConfState at some index and make sure that this index can't be reached
	// from log position 1, so that followers are forced to pick up the
	// ConfState in order to move away from log position 1 (unless they got
	// bootstrapped in the same way already). Failing to do so would mean that
	// followers diverge from the bootstrapped nodes and don't learn about the
	// initial config.
	//
	// NB: this is exactly what CockroachDB does. The Raft log really begins at
	// index 10, so empty followers (at index 1) always need a snapshot first.
  auto bootstrap = [](std::shared_ptr<craft::MemoryStorage> storage, raftpb::ConfState cs) {
    if (cs.voters_size() == 0) {
      return craft::Status::Error("no voters specified");
    }
    auto [fi, s1] = storage->FirstIndex();
    if (!s1.IsOK()) {
      return s1;
    }
    if (fi < 2) {
      return craft::Status::Error("FirstIndex >= 2 is prerequisite for bootstrap");
    }
    std::tie(std::ignore, s1) = storage->Entries(fi, fi, craft::Raft::kNoLimit);
    if (s1.IsOK()) {
      return craft::Status::Error("should not have been able to load first index");
    }

    auto [li, s2] = storage->LastIndex();
    if (!s2.IsOK()) {
      return s2;
    }
    std::tie(std::ignore, s2) = storage->Entries(li, li, craft::Raft::kNoLimit);
    if (s2.IsOK()) {
      return craft::Status::Error("should not have been able to load last index");
    }
    auto [hs, ics, s3] = storage->InitialState();
    if (!s3.IsOK()) {
      return s3;
    }
    if (!craft::IsEmptyHardState(hs)) {
      return craft::Status::Error("HardState not empty");
    }
    if (ics.voters_size() != 0) {
      return craft::Status::Error("ConfState not empty");
    }

    auto snap = std::make_shared<raftpb::Snapshot>();
    snap->mutable_metadata()->set_index(1);
    snap->mutable_metadata()->set_term(1);
    *snap->mutable_metadata()->mutable_conf_state() = cs;
    return storage->ApplySnapshot(snap);
  };

  auto status = bootstrap(storage, *NEW_CONF_STATE().Voters({1})());
  ASSERT_TRUE(status.IsOK());

  auto rawnode = craft::RawNode::New(newTestConfig(1, 10, 1, storage));
  ASSERT_FALSE(rawnode->HasReady());
  rawnode->Campaign();
  rawnode->Propose("foo");
  ASSERT_TRUE(rawnode->HasReady());

  auto rd = rawnode->GetReady();
  storage->Append(rd->entries);
  rawnode->Advance();

  rd->soft_state.reset();
  want.soft_state.reset();

  readyEqual(*rd, want);
  ASSERT_FALSE(rawnode->HasReady());
}

TEST(RawNode, Restart) {
  craft::EntryPtrs entries = {
    NEW_ENT().Term(1).Index(1)(),
    NEW_ENT().Term(1).Index(2).Data("foo")(),
  };
  raftpb::HardState st;
  st.set_term(1);
  st.set_commit(1);

  craft::Ready want = {
    // commit up to commit index in st
    .committed_entries = {NEW_ENT().Term(1).Index(1)()},
    .must_sync = false,
  };

  auto storage = newTestMemoryStorage({withPeers({1})});
  storage->SetHardState(st);
  storage->Append(entries);
  auto rawnode = craft::RawNode::New(newTestConfig(1, 10, 1, storage));
  auto rd = rawnode->GetReady();
  readyEqual(*rd, want);
  rawnode->Advance();
  ASSERT_FALSE(rawnode->HasReady());
}

TEST(RawNode, RestartFromSnapshot) {
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(2);
  snap->mutable_metadata()->set_term(1);
  snap->mutable_metadata()->mutable_conf_state()->add_voters(1);
  snap->mutable_metadata()->mutable_conf_state()->add_voters(2);
  craft::EntryPtrs entries = {
    NEW_ENT().Term(1).Index(3).Data("foo")(),
  };
  raftpb::HardState st;
  st.set_term(1);
  st.set_commit(3);

  craft::Ready want = {
    // commit up to commit index in st
    .committed_entries = entries,
    .must_sync = false,
  };

  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto s = std::make_shared<craft::MemoryStorage>(logger);
  s->SetHardState(st);
  s->ApplySnapshot(snap);
  s->Append(entries);
  auto rawnode = craft::RawNode::New(newTestConfig(1, 10, 1, s));
  auto rd = rawnode->GetReady();
  readyEqual(*rd, want);
  rawnode->Advance();
  ASSERT_FALSE(rawnode->HasReady());
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()
TEST(RawNode, NodeStatus) {
  auto s = newTestMemoryStorage({withPeers({1})});
  auto rn = craft::RawNode::New(newTestConfig(1, 10, 1, s));
  auto status = rn->GetStatus();
  ASSERT_EQ(status.progress.empty(), true);

  auto s1 = rn->Campaign();
  ASSERT_TRUE(s1.IsOK());
  status = rn->GetStatus();
  ASSERT_EQ(status.basic.soft_state.lead, static_cast<uint64_t>(1));
  ASSERT_EQ(status.basic.soft_state.raft_state, craft::RaftStateType::kLeader);
  auto exp = rn->GetRaft()->GetTracker().GetProgress(1);
  auto act = status.progress[1];
  ASSERT_EQ(exp->String(), act->String());

  craft::ProgressTracker::Config exp_cfg;
  exp_cfg.voters.Incoming().Add(1);
  ASSERT_EQ(status.config, exp_cfg);
}

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
// - node learns that index 11 is committed
// - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
//   exceeds maxBytes), which isn't noticed internally by Raft
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path and removes the last entry.
// - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//   its internal applied index cursor to 10 (when it should be 9)
// - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//    write.
TEST(RawNode, CommitPaginationAfterRestart) {
  auto s = newTestMemoryStorage({withPeers({1})});
  s->SetIgnoreSize(true);

  raftpb::HardState persisted_hardState;
  persisted_hardState.set_term(1);
  persisted_hardState.set_vote(1);
  persisted_hardState.set_commit(10);

  s->SetHardState(persisted_hardState);
  craft::EntryPtrs ents;
  size_t size;
  for (size_t i = 0; i < 10; i++) {
    auto ent = NEW_ENT().Term(1).Index(i + 1).Type(raftpb::EntryType::EntryNormal).Data("a")();
    ents.push_back(ent);
    size += ent->ByteSizeLong();
  }
  s->SetEntries(ents);

  auto cfg = newTestConfig(1, 10, 1, s);
	// Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
	// not be included in the initial rd.CommittedEntries. However, our storage will ignore
	// this and *will* return it (which is how the Commit index ended up being 10 initially).
  cfg.max_size_per_msg = size - (*ents.rbegin())->ByteSizeLong() - 1;
  ents.push_back(NEW_ENT().Term(1).Index(11).Type(raftpb::EntryType::EntryNormal).Data("boom")());
  s->SetEntries(ents);

  auto rawnode = craft::RawNode::New(cfg);
  for (uint64_t highest_applied = 0; highest_applied != 11;) {
    auto rd = rawnode->GetReady();
    auto n = rd->committed_entries.size();
    ASSERT_NE(n, static_cast<size_t>(0));
    auto next = rd->committed_entries[0]->index();
    ASSERT_FALSE(highest_applied != 0 && highest_applied + 1 != next);
    highest_applied = rd->committed_entries[n-1]->index();
    rawnode->Advance();
    rawnode->Step(NEW_MSG()
                  .Type(raftpb::MessageType::MsgHeartbeat)
                  .To(1)
                  .From(1)
                  .Term(1)  // illegal, but we get away with it
                  .Commit(11)());
  }
}

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
TEST(RawNode, BoundedLogGrowthWithPartition) {
  size_t max_entries = 16;
  std::string data = "testdata";
  auto test_entry = NEW_ENT().Data(data)();
  uint64_t max_entry_size = max_entries * craft::Util::PayloadSize(test_entry);

  auto s = newTestMemoryStorage({withPeers({1})});
  auto cfg = newTestConfig(1, 10, 1, s);
  cfg.max_uncommitted_entries_size = max_entry_size;
  auto rawnode = craft::RawNode::New(cfg);
  auto rd = rawnode->GetReady();
  s->Append(rd->entries);
  rawnode->Advance();

  // Become the leader.
  rawnode->Campaign();
  while (1) {
    rd = rawnode->GetReady();
    s->Append(rd->entries);
    if (rd->soft_state->lead == rawnode->GetRaft()->ID()) {
      rawnode->Advance();
      break;
    }
    rawnode->Advance();
  }

	// Simulate a network partition while we make our proposals by never
	// committing anything. These proposals should not cause the leader's
	// log to grow indefinitely.
  for (size_t i = 0; i < 1024; i++) {
    rawnode->Propose(data);
  }

	// Check the size of leader's uncommitted log tail. It should not exceed the
	// MaxUncommittedEntriesSize limit.
  auto check_uncommitted = [&rawnode](uint64_t exp) {
    ASSERT_EQ(rawnode->GetRaft()->UncommittedSize(), exp);
  };
  check_uncommitted(max_entry_size);

	// Recover from the partition. The uncommitted tail of the Raft log should
	// disappear as entries are committed.
  rd = rawnode->GetReady();
  ASSERT_EQ(rd->committed_entries.size(), max_entries);
  s->Append(rd->entries);
  rawnode->Advance();
  check_uncommitted(0);
}

TEST(RawNode, ConsumeReady) {
	// Check that readyWithoutAccept() does not call acceptReady (which resets
	// the messages) but Ready() does.
  auto s = newTestMemoryStorage({withPeers({1})});
  auto rn = craft::RawNode::New(newTestConfig(1, 3, 1, s));
  auto m1 = NEW_MSG().Context("foo")();
  auto m2 = NEW_MSG().Context("bar")();

  // Inject first message, make sure it's visible via readyWithoutAccept.
  rn->GetRaft()->GetMsgsForTest().push_back(m1);
  auto rd = rn->ReadyWithoutAccept();
  ASSERT_EQ(rd->messages.size(), static_cast<size_t>(1));
  ASSERT_EQ(rd->messages[0]->context(), m1->context());

  ASSERT_EQ(rn->GetRaft()->Msgs().size(), static_cast<size_t>(1));
  ASSERT_EQ(rn->GetRaft()->Msgs()[0]->context(), m1->context());

	// Now call Ready() which should move the message into the Ready (as opposed
	// to leaving it in both places).
  rd = rn->GetReady();
  ASSERT_EQ(rn->GetRaft()->Msgs().size(), static_cast<size_t>(0));
  ASSERT_EQ(rd->messages.size(), static_cast<size_t>(1));
  ASSERT_EQ(rd->messages[0]->context(), m1->context());
  // Add a message to raft to make sure that Advance() doesn't drop it.
  rn->GetRaft()->GetMsgsForTest().push_back(m2);
  rn->Advance();
  ASSERT_EQ(rn->GetRaft()->Msgs().size(), static_cast<size_t>(1));
  ASSERT_EQ(rn->GetRaft()->Msgs()[0]->context(), m2->context());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}