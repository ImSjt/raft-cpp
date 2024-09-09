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

// TestMsgAppFlowControlFull ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.

TEST(Raft, MsgAppFlowControlFull) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  auto pr2 = r->Get()->GetTracker().GetProgress(2);
  // force the progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (int64_t i = 0; i < r->Get()->GetTracker().MaxInflight(); i++) {
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    auto ms = r->ReadMessages();
    ASSERT_EQ(ms.size(), static_cast<size_t>(1));
  }

  // ensure 1
  ASSERT_TRUE(pr2->GetInflights()->Full());

  // ensure 2
  for (int64_t i = 0; i < 10; i++) {
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    auto ms = r->ReadMessages();
    ASSERT_EQ(ms.size(), static_cast<size_t>(0));
  }
}

// TestMsgAppFlowControlMoveForward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
TEST(Raft, MsgAppFlowControlMoveForward) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  auto pr2 = r->Get()->GetTracker().GetProgress(2);
  // force the progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (int64_t i = 0; i < r->Get()->GetTracker().MaxInflight(); i++) {
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    auto ms = r->ReadMessages();
    ASSERT_EQ(ms.size(), static_cast<size_t>(1));
  }

	// 1 is noop, 2 is the first proposal we just sent.
	// so we start with 2.
  for (int64_t tt = 2; tt < r->Get()->GetTracker().MaxInflight(); tt++) {
    // move forward the window
    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(tt)());
    r->ReadMessages();

    ASSERT_EQ(pr2->GetInflights()->Full(), false);

    // fill in the inflights window again
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    auto ms = r->ReadMessages();
    ASSERT_EQ(ms.size(), static_cast<size_t>(1));

    // ensure 1
    ASSERT_EQ(pr2->GetInflights()->Full(), true);

    // ensure 2
    for (int64_t i = 0; i < tt; i++) {
      r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(i)());
      ASSERT_EQ(pr2->GetInflights()->Full(), true);
    }
  }
}

// TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response
// frees one slot if the window is full.
TEST(Raft, MsgAppFlowControlRecvHeartbeat) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  auto pr2 = r->Get()->GetTracker().GetProgress(2);
  // force the progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (int64_t i = 0; i < r->Get()->GetTracker().MaxInflight(); i++) {
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    r->ReadMessages();
  }

  for (int tt = 1; tt < 5; tt++) {
    ASSERT_EQ(pr2->GetInflights()->Full(), true);

    for (int i = 0; i < tt; i++) {
      // recv tt msgHeartbeatResp and expect one free slot
      r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)());
      r->ReadMessages();
      ASSERT_EQ(pr2->GetInflights()->Full(), false);
    }

    // one slot
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
    auto ms = r->ReadMessages();
    ASSERT_EQ(ms.size(), static_cast<size_t>(1));

		// and just one slot
    for (int i = 0; i < 10; i++) {
      r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
      auto ms1 = r->ReadMessages();
      ASSERT_EQ(ms1.size(), static_cast<size_t>(0));
    }

    // clear all pending messages.
    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)());
    r->ReadMessages();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}