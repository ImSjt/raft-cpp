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

static craft::SnapshotPtr makeTestingSnap() {
  auto snap = std::make_shared<raftpb::Snapshot>();
  snap->mutable_metadata()->set_index(11);
  snap->mutable_metadata()->set_term(11);
  snap->mutable_metadata()->mutable_conf_state()->add_voters(1);
  snap->mutable_metadata()->mutable_conf_state()->add_voters(2);
  return snap;
}

TEST(Raft, SendingSnapshotSetPendingSnapshot) {
  auto storage = newTestMemoryStorage({withPeers({1})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(makeTestingSnap());

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

	// force set the next of node 2, so that
	// node 2 needs a snapshot
  sm->Get()->GetTracker().GetProgress(2)->SetNext(sm->Get()->GetRaftLog()->FirstIndex());

  sm->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(sm->Get()->GetTracker().GetProgress(2)->Next() - 1).Reject(true)());
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->PendingSnapshot(), static_cast<uint64_t>(11));
}

TEST(Raft, PendingSnapshotPauseReplication) {
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(makeTestingSnap());

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

	// force set the next of node 2, so that
	// node 2 needs a snapshot
  sm->Get()->GetTracker().GetProgress(2)->BecomeSnapshot(11);

  sm->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(0));
}

TEST(Raft, SnapshotFailure) {
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(makeTestingSnap());

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

  sm->Get()->GetTracker().GetProgress(2)->SetNext(1);
  sm->Get()->GetTracker().GetProgress(2)->BecomeSnapshot(11);

  sm->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgSnapStatus).Reject(true)());
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->PendingSnapshot(), static_cast<uint64_t>(0));
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->Next(), static_cast<uint64_t>(1));
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->ProbeSent(), true);
}

TEST(Raft, SnapshotSucceed) {
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(makeTestingSnap());

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

  sm->Get()->GetTracker().GetProgress(2)->SetNext(1);
  sm->Get()->GetTracker().GetProgress(2)->BecomeSnapshot(11);

  sm->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgSnapStatus).Reject(true)());
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->PendingSnapshot(), static_cast<uint64_t>(0));
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->Next(), static_cast<uint64_t>(1));
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->ProbeSent(), true);
}

TEST(Raft, SnapshotAbort) {
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(makeTestingSnap());

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

  sm->Get()->GetTracker().GetProgress(2)->SetNext(1);
  sm->Get()->GetTracker().GetProgress(2)->BecomeSnapshot(11);

	// A successful msgAppResp that has a higher/equal index than the
	// pending snapshot should abort the pending snapshot.
  sm->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(11)());
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->PendingSnapshot(), static_cast<uint64_t>(0));

	// The follower entered StateReplicate and the leader send an append
	// and optimistically updated the progress (so we see 13 instead of 12).
	// There is something to append because the leader appended an empty entry
	// to the log at index 12 when it assumed leadership.
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->Next(), static_cast<uint64_t>(13));
  ASSERT_EQ(sm->Get()->GetTracker().GetProgress(2)->GetInflights()->Count(), static_cast<int64_t>(1));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}