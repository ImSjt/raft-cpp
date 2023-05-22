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
#include <vector>
#include <random>

#include "gtest/gtest.h"
#include "raft.h"
#include "rawnode.h"
#include "util.h"
#include "raftpb/confchange.h"
#include "raft_test_util.h"

TEST(Raft, ProgressLeader) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();

  // Send proposals to r1. The first 5 entries should be appended to the log.
  auto prop_msg = NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("foo")()})();
  for (size_t i = 0; i < 5; i++) {
    auto pr = r->Get()->GetTracker().GetProgress(r->Get()->ID());
    ASSERT_EQ(pr->State(), craft::StateType::kReplicate);
    ASSERT_EQ(pr->Match(), static_cast<uint64_t>(i+1));
    ASSERT_EQ(pr->Next(), pr->Match() + 1);
    auto s = r->Step(prop_msg);
    ASSERT_TRUE(s.IsOK());
  }
}

TEST(Raft, ProgressResumeByHeartbeatResp) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  r->Get()->GetTracker().GetProgress(2)->SetProbeSent(true);

  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)());
  ASSERT_TRUE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());

  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)());
  ASSERT_FALSE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());
}

TEST(Raft, ProgressPaused) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());

  auto ms = r->ReadMessages();
  ASSERT_EQ(ms.size(), static_cast<size_t>(1));
}

TEST(Raft, ProgressFlowControl) {
  auto cfg = newTestConfig(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  cfg.max_inflight_msgs = 3;
  cfg.max_size_per_msg = 2048;
  auto r = craft::Raft::New(cfg);
  r->BecomeCandidate();
  r->BecomeLeader();

  // Throw away all the messages relating to the initial election.
  r->ClearMsgs();

  // While node 2 is in probe state, propose a bunch of entries.
  r->GetTracker().GetProgress(2)->BecomeProbe();
  std::string blob(1000, 'a');
  for (size_t i = 0; i < 10; i++) {
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data(blob)()})());
  }

  auto ms = r->ReadAndClearMsgs();
	// First append has two entries: the empty entry to confirm the
	// election, and the first proposal (only one proposal gets sent
	// because we're in probe state).
  ASSERT_EQ(ms.size(), static_cast<size_t>(1));
  ASSERT_EQ(ms[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(ms[0]->entries().size(), 2);
  ASSERT_EQ(ms[0]->entries(0).data().size(), static_cast<size_t>(0));
  ASSERT_EQ(ms[0]->entries(1).data().size(), static_cast<size_t>(1000));

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(ms[0]->entries(1).index())());
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), static_cast<size_t>(3));
  for (auto m : ms) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
    ASSERT_EQ(m->entries().size(), 2);
  }

	// Ack all three of those messages together and get the last two
	// messages (containing three entries).
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(ms[2]->entries(1).index())());
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), static_cast<size_t>(2));
  for (auto m : ms) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
  }
  ASSERT_EQ(ms[0]->entries().size(), 2);
  ASSERT_EQ(ms[1]->entries().size(), 1);
}

TEST(Raft, UncommittedEntryLimit) {
	// Use a relatively large number of entries here to prevent regression of a
	// bug which computed the size before it was fixed. This test would fail
	// with the bug, either because we'd get dropped proposals earlier than we
	// expect them, or because the final tally ends up nonzero. (At the time of
	// writing, the former).
  size_t max_entries = 1024;
  auto test_entry = NEW_ENT().Data("testdata")();
  size_t max_entry_size = max_entries * craft::Util::PayloadSize(test_entry);

  ASSERT_EQ(craft::Util::PayloadSize(std::make_shared<raftpb::Entry>()), static_cast<size_t>(0));

  auto cfg = newTestConfig(1, 4, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  cfg.max_uncommitted_entries_size = static_cast<uint64_t>(max_entry_size);
  cfg.max_inflight_msgs = 2 * 1024;  // avoid interference
  auto r = craft::Raft::New(cfg);
  r->BecomeCandidate();
  r->BecomeLeader();
  ASSERT_EQ(r->UncommittedSize(), static_cast<uint64_t>(0));

  // Set the two followers to the replicate state. Commit to tail of log.
  size_t num_followers = 2;
  r->GetTracker().GetProgress(2)->BecomeReplicate();
  r->GetTracker().GetProgress(3)->BecomeReplicate(); 
  r->SetUncommittedSize(0);

  // Send proposals to r1. The first 5 entries should be appended to the log.
  craft::EntryPtrs prop_ents;
  for (size_t i = 0; i < max_entries; i++) {
    auto s = r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({test_entry})());
    ASSERT_TRUE(s.IsOK());
    prop_ents.push_back(test_entry);
  }

  // Send one more proposal to r1. It should be rejected.
  auto s = r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({test_entry})());
  ASSERT_FALSE(s.IsOK());
  ASSERT_STREQ(s.Str(), craft::kErrProposalDropped);

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
  auto ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), max_entries * num_followers);
  r->ReduceUncommittedSize(prop_ents);
  ASSERT_EQ(r->UncommittedSize(), static_cast<uint64_t>(0));

	// Send a single large proposal to r1. Should be accepted even though it
	// pushes us above the limit because we were beneath it before the proposal.
  prop_ents.clear();
  for (size_t i = 0; i < max_entry_size * 2; i++) {
    prop_ents.push_back(test_entry);
  }

  s = r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries(prop_ents)());
  ASSERT_TRUE(s.IsOK());

  // Send one more proposal to r1. It should be rejected, again.
  s = r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({test_entry})());
  ASSERT_FALSE(s.IsOK());
  ASSERT_STREQ(s.Str(), craft::kErrProposalDropped);

	// But we can always append an entry with no Data. This is used both for the
	// leader's first empty entry and for auto-transitioning out of joint config
	// states.
  s = r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("")()})());
  ASSERT_TRUE(s.IsOK());

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), num_followers * 2);
  r->ReduceUncommittedSize(prop_ents);
  ASSERT_EQ(r->UncommittedSize(), static_cast<uint64_t>(0));
}

static void testLeaderElection(bool pre_vote) {
  NetWork::ConfigFunc cfg;
  auto cand_state = craft::RaftStateType::kCandidate;
  uint64_t cand_term = 1;
  if (pre_vote) {
    cfg = preVoteConfig;
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
    cand_state = craft::RaftStateType::kPreCandidate;
    cand_term = 0;
  }

  struct Test {
    std::shared_ptr<NetWork> network;
    craft::RaftStateType state;
    uint64_t exp_term;
  };
  std::vector<Test> tests = {
      {NetWork::NewWithConfig(cfg, {nullptr, nullptr, nullptr}),
       craft::RaftStateType::kLeader, 1},
      {NetWork::NewWithConfig(cfg, {nullptr, nullptr, BlackHole::New()}),
       craft::RaftStateType::kLeader, 1},
      {NetWork::NewWithConfig(cfg,
                              {nullptr, BlackHole::New(), BlackHole::New()}),
       cand_state, cand_term},
      {NetWork::NewWithConfig(
           cfg, {nullptr, BlackHole::New(), BlackHole::New(), nullptr}),
       cand_state, cand_term},
      {NetWork::NewWithConfig(cfg, {nullptr, BlackHole::New(), BlackHole::New(),
                                    nullptr, nullptr}),
       craft::RaftStateType::kLeader, 1},
      // three logs further along than 0, but in the same term so rejections
      // are returned instead of the votes being ignored.
      {NetWork::NewWithConfig(
           cfg, {nullptr, entsWithConfig(cfg, {1}), entsWithConfig(cfg, {1}),
            entsWithConfig(cfg, {1, 1}), nullptr}), craft::RaftStateType::kFollower, 1},
  };

  for (auto& tt : tests) {
    tt.network->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
    auto sm = std::dynamic_pointer_cast<Raft>(tt.network->Peers()[1])->Get();
    ASSERT_EQ(sm->State(), tt.state);
    ASSERT_EQ(sm->Term(), tt.exp_term);
  }
}

TEST(Raft, LeaderElection) {
  testLeaderElection(false);
}

TEST(Raft, LeaderElectionPreVote) {
  testLeaderElection(true);
}

// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
TEST(Raft, LearnerElectionTimeout) {
  auto n1 = newTestLearnerRaft(1, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));
  auto n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);

	// n2 is learner. Learner should not start election even when times out.
  n2->Get()->SetRandomizedElectionTimeout(n2->Get()->ElectionTimeout());
  for (int64_t i = 0; i < n2->Get()->ElectionTimeout(); i++) {
    n2->Get()->Tick();
  }

  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
TEST(Raft, LearnerPromotion) {
  auto n1 = newTestLearnerRaft(1, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));
  auto n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);

  auto nt = NetWork::New({n1, n2});

  ASSERT_NE(n1->Get()->State(), craft::RaftStateType::kLeader);

  // n1 should become leader
  n1->Get()->SetRandomizedElectionTimeout(n1->Get()->ElectionTimeout());
  for (int64_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)()});

  n1->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  n2->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  ASSERT_FALSE(n2->Get()->IsLearner());

  // n2 start election, should become leader
  n2->Get()->SetRandomizedElectionTimeout(n2->Get()->ElectionTimeout());
  for (int64_t i = 0; i < n2->Get()->ElectionTimeout(); i++) {
    n2->Get()->Tick();
  }

  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgBeat)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kLeader);
}

// TestLearnerCanVote checks that a learner can vote when it receives a valid Vote request.
// See (*raft).Step for why this is necessary and correct behavior.
TEST(Raft, LearnerCanVote) {
  auto n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  n2->Get()->BecomeFollower(1, craft::Raft::kNone);

  auto m = std::make_shared<raftpb::Message>();
  m->set_from(1);
  m->set_to(2);
  m->set_term(2);
  m->set_type(raftpb::MessageType::MsgVote);
  m->set_logterm(11);
  m->set_index(11);
  n2->Step({m});

  ASSERT_EQ(n2->Get()->Msgs().size(), static_cast<size_t>(1));
  auto msg = n2->Get()->Msgs()[0];
  ASSERT_EQ(msg->type(), raftpb::MessageType::MsgVoteResp);
  ASSERT_FALSE(msg->reject());
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
static void testLeaderCycle(bool pre_vote) {
  NetWork::ConfigFunc cfg;
  if (pre_vote) {
    cfg = preVoteConfig;
  }
  auto n = NetWork::NewWithConfig(cfg, {nullptr, nullptr, nullptr});
  for (uint64_t campaigner_id = 1; campaigner_id <= 3; campaigner_id++) {
    n->Send({NEW_MSG().From(campaigner_id).To(campaigner_id).Type(raftpb::MessageType::MsgHup)()});

    for (auto& p : n->Peers()) {
      auto sm = std::dynamic_pointer_cast<Raft>(p.second);
      if (sm->Get()->ID() == campaigner_id) {
        ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
      } else if (sm->Get()->ID() != campaigner_id) {
        ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);
      }
    }
  }
}

TEST(Raft, LeaderCycle) {
  testLeaderCycle(false);
}

TEST(Raft, LeaderCyclePreVote) {
  testLeaderCycle(true);
}

static void testLeaderElectionOverwriteNewerLogs(bool pre_vote) {
  NetWork::ConfigFunc cfg;
  if (pre_vote) {
    cfg = preVoteConfig;
  }
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
  auto n = NetWork::NewWithConfig(cfg, {
    entsWithConfig(cfg, {1}),   // Node 1: Won first election
    entsWithConfig(cfg, {1}),   // Node 2: Got logs from node 1
    entsWithConfig(cfg, {2}),   // Node 3: Won second election
    votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
    votedWithConfig(cfg, 3, 2), // Node 5: Voted but didn't get logs
  });

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
  n->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  auto sm1 = std::dynamic_pointer_cast<Raft>(n->Peers()[1]);
  ASSERT_EQ(sm1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(sm1->Get()->Term(), static_cast<uint64_t>(2));

  // Node 1 campaigns again with a higher term. This time it succeeds.
  n->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(sm1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(sm1->Get()->Term(), static_cast<uint64_t>(3));

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
  for (auto& p : n->Peers()) {
    auto sm = std::dynamic_pointer_cast<Raft>(p.second);
    auto entries = sm->Get()->GetRaftLog()->AllEntries();
    ASSERT_EQ(entries.size(), static_cast<size_t>(2));
    ASSERT_EQ(entries[0]->term(), static_cast<uint64_t>(1));
    ASSERT_EQ(entries[1]->term(), static_cast<uint64_t>(3));
  }
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
TEST(Raft, LeaderElectionOverwriteNewerLogs) {
  testLeaderElectionOverwriteNewerLogs(false);
}

TEST(Raft, LeaderElectionOverwriteNewerLogsPreVote) {
  testLeaderElectionOverwriteNewerLogs(true);
}

static void testVoteFromAnyState(raftpb::MessageType vt) {
  for (uint8_t st = 0; st < craft::RaftStateType::kNumState; st++) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    r->Get()->SetTerm(1);

    switch (st) {
      case craft::RaftStateType::kFollower:
        r->Get()->BecomeFollower(r->Get()->Term(), 3);
        break;
      case craft::RaftStateType::kPreCandidate:
        r->Get()->BecomePreCandidate();
        break;
      case craft::RaftStateType::kCandidate:
        r->Get()->BecomeCandidate();
        break;
      case craft::RaftStateType::kLeader:
        r->Get()->BecomeCandidate();
        r->Get()->BecomeLeader();
        break;
    }

		// Note that setting our state above may have advanced r.Term
		// past its initial value.
    auto orig_term = r->Get()->Term();
    auto new_term = r->Get()->Term() + 1;
    auto s = r->Step(NEW_MSG().From(2).To(1).Type(vt).Term(new_term).LogTerm(new_term).Index(42)());
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(r->Get()->Msgs().size(), static_cast<size_t>(1));
    auto resp = r->Get()->Msgs()[0];
    ASSERT_EQ(resp->type(), craft::Util::VoteRespMsgType(vt));
    ASSERT_FALSE(resp->reject());

		// If this was a real vote, we reset our state and term.
    if (vt == raftpb::MessageType::MsgVote) {
      ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
      ASSERT_EQ(r->Get()->Term(), new_term);
      ASSERT_EQ(r->Get()->Vote(), static_cast<uint64_t>(2));
    } else {
      // In a prevote, nothing changes.
      ASSERT_EQ(r->Get()->State(), st);
      ASSERT_EQ(r->Get()->Term(), orig_term);
			// if st == craft::RaftStateType::kFollower or craft::RaftStateType::kPreCandidate, r hasn't voted yet.
			// In craft::RaftStateType::kCandidate or craft::RaftStateType::kLeader, it's voted for itself.
      ASSERT_FALSE(r->Get()->Vote() != craft::Raft::kNone &&
                   r->Get()->Vote() != 1) << raftpb::MessageType_Name(vt) << "," <<
                   craft::RaftStateTypeName(static_cast<craft::RaftStateType>(st)) << ": vote " << r->Get()->Vote() <<
                   ", want " << craft::Raft::kNone << " or 1";
    }
  }
}

TEST(Raft, VoteFromAnyState) {
  testVoteFromAnyState(raftpb::MessageType::MsgVote);
}

TEST(Raft, PreVoteFromAnyState) {
  testVoteFromAnyState(raftpb::MessageType::MsgPreVote);
}

// nextEnts returns the appliable entries and updates the applied index
static craft::EntryPtrs nextEnts(craft::Raft* raft, std::shared_ptr<craft::MemoryStorage> s) {
	// Transfer all unstable entries to "stable" storage.
  s->Append(raft->GetRaftLog()->UnstableEntries());
  raft->GetRaftLog()->StableTo(raft->GetRaftLog()->LastIndex(), raft->GetRaftLog()->LastTerm());

  auto ents = raft->GetRaftLog()->NextEnts();
  raft->GetRaftLog()->AppliedTo(raft->GetRaftLog()->Committed());
  return ents;
}

TEST(Raft, LogReplication) {
  struct Test {
    std::shared_ptr<NetWork> network;
    craft::MsgPtrs msgs;
    uint64_t wcommitted;
  };
  std::vector<Test> tests = {
    {
      NetWork::New({nullptr, nullptr, nullptr}),
      {NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})()},
      2,
    },
    {
      NetWork::New({nullptr, nullptr, nullptr}),
      {
        NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})(),
        NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgHup)(),
        NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})(),
      },
      4,
    }
  };

  for (auto& tt : tests) {
    craft::MsgPtrs props;
    for (auto& m : tt.msgs) {
      if (m->type() == raftpb::MessageType::MsgProp) {
        props.emplace_back(std::make_shared<raftpb::Message>(*m));
      }
    }

    tt.network->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
    for (auto& m : tt.msgs) {
      tt.network->Send({m});
    }

    for (auto& p : tt.network->Peers()) {
      auto sm = std::dynamic_pointer_cast<Raft>(p.second);
      ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), tt.wcommitted);

      craft::EntryPtrs ents;
      for (auto& e : nextEnts(sm->Get(), tt.network->Storages()[p.first])) {
        if (!e->data().empty()) {
          ents.emplace_back(e);
        }
      }
      for (size_t i = 0; i < props.size(); i++) {
        ASSERT_EQ(ents[i]->data(), props[i]->entries(0).data());
      }
    }
  }
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
TEST(Raft, LearnerLogReplication) {
  auto n1 = newTestLearnerRaft(1, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));
  auto n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  auto nt = NetWork::New({n1, n2});

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);

  n1->Get()->SetRandomizedElectionTimeout(n1->Get()->ElectionTimeout());
  for (int64_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)()});

	// n1 is leader and n2 is learner
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_TRUE(n2->Get()->IsLearner());

  auto next_committed = n1->Get()->GetRaftLog()->Committed() + 1;
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})()});
  ASSERT_EQ(n1->Get()->GetRaftLog()->Committed(), next_committed);
  ASSERT_EQ(n1->Get()->GetRaftLog()->Committed(), n2->Get()->GetRaftLog()->Committed());

  auto match = n1->Get()->GetTracker().GetProgress(2)->Match();
  ASSERT_EQ(match, n2->Get()->GetRaftLog()->Committed());
}

TEST(Raft, SingleNodeCommit) {
  auto tt = NetWork::New({nullptr});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(3));
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
TEST(Raft, CannotCommitWithoutNewTermEntry) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr, nullptr, nullptr});
  // tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  // 1 cannot reach 3,4,5
  tt->Cut(1, 3);
  tt->Cut(1, 4);
  tt->Cut(1, 5);

  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(1));

  // network recovery
  tt->Recover();
  // avoid committing ChangeTerm proposal
  tt->Ignore(raftpb::MessageType::MsgApp);

  // elect 2 as the new leader with term 2
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

  // no log entries from previous term should be committed
  sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[2]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(1));

  tt->Recover();
  // send heartbeat; reset wait
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgBeat)()});
  // append an entry at cuttent term
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  // expect the committed to be advanced
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(5));
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
TEST(Raft, CommitWithoutNewTermEntry) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr, nullptr, nullptr});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  // 1 cannot reach 3,4,5
  tt->Cut(1, 3);
  tt->Cut(1, 4);
  tt->Cut(1, 5);

  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(1));

  // network recovery
  tt->Recover();

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(4));
}

TEST(Raft, DuelingCandidates) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  auto nt = NetWork::New({a, b, c});
  nt->Cut(1, 3);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  // 1 becomes leader since it receives votes from 1 and 2
  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kCandidate);

  nt->Recover();

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({NEW_ENT().Index(0).Term(0)(), NEW_ENT().Index(1).Term(1)()});
  auto wlog = std::make_shared<craft::RaftLog>(storage);
  wlog->SetCommitted(1);
  wlog->GetUnstable().SetOffset(2);

  struct Test {
    std::shared_ptr<Raft> sm;
    craft::RaftStateType state;
    uint64_t term;
    std::shared_ptr<craft::RaftLog> raftlog;
  };
  std::vector<Test> tests = {
    {a, craft::RaftStateType::kFollower, 2, wlog},
    {b, craft::RaftStateType::kFollower, 2, wlog},
    {c, craft::RaftStateType::kFollower, 2, craft::RaftLog::New(std::make_shared<craft::MemoryStorage>())},
  };
  for (size_t i = 0; i < tests.size(); i++) {
    auto& tt = tests[i];
    ASSERT_EQ(tt.sm->Get()->State(), tt.state);
    ASSERT_EQ(tt.sm->Get()->Term(), tt.term);
    auto base = raftlogString(tt.raftlog.get());
    auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1+i]);
    ASSERT_TRUE(sm != nullptr);
    ASSERT_EQ(raftlogString(sm->Get()->GetRaftLog()), base);
  }
}

TEST(Raft, DuelingPreCandidates) {
  auto cfga = newTestConfig(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto cfgb = newTestConfig(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto cfgc = newTestConfig(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  cfga.pre_vote = true;
  cfgb.pre_vote = true;
  cfgc.pre_vote = true;
  auto a = newTestRaftWithConfig(cfga);
  auto b = newTestRaftWithConfig(cfgb);
  auto c = newTestRaftWithConfig(cfgc);

  auto nt = NetWork::New({a, b, c});
  nt->Cut(1, 3);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  // 1 becomes leader since it receives votes from 1 and 2
  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  // 3 campaigns then reverts to follower when its PreVote is rejected
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

  nt->Recover();

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({NEW_ENT().Index(0).Term(0)(), NEW_ENT().Index(1).Term(1)()});
  auto wlog = std::make_shared<craft::RaftLog>(storage);
  wlog->SetCommitted(1);
  wlog->GetUnstable().SetOffset(2);
  struct Test {
    std::shared_ptr<Raft> sm;
    craft::RaftStateType state;
    uint64_t term;
    std::shared_ptr<craft::RaftLog> raftlog;
  };
  std::vector<Test> tests = {
    {a, craft::RaftStateType::kLeader, 1, wlog},
    {b, craft::RaftStateType::kFollower, 1, wlog},
    {c, craft::RaftStateType::kFollower, 1, craft::RaftLog::New(std::make_shared<craft::MemoryStorage>())},
  };
  for (size_t i = 0; i < tests.size(); i++) {
    auto& tt = tests[i];
    ASSERT_EQ(tt.sm->Get()->State(), tt.state);
    ASSERT_EQ(tt.sm->Get()->Term(), tt.term);
    auto base = raftlogString(tt.raftlog.get());
    auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1+i]);
    ASSERT_TRUE(sm != nullptr);
    ASSERT_EQ(raftlogString(sm->Get()->GetRaftLog()), base);
  }
}

TEST(Raft, CandidateConcede) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr});
  tt->Isolate(1);

  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  tt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  // heal the partition
  tt->Recover();
  // send heartbeat; reset wait
  tt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgBeat)()});

  std::string data = "force follower";
  // send a proposal to 3 to flush out a MsgApp to 1
  tt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data(data)()})()});
  // send heartbeat; flush out commit
  tt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgBeat)()});

  auto a = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(a->Get()->Term(), static_cast<uint64_t>(1));

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({NEW_ENT().Index(0).Term(0)(), NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1).Data(data)()});
  auto l = std::make_shared<craft::RaftLog>(storage);
  l->SetCommitted(2);
  l->GetUnstable().SetOffset(3);
  auto want_log = raftlogString(l.get());
  for (auto& p  : tt->Peers()) {
    auto sm = std::dynamic_pointer_cast<Raft>(p.second);
    ASSERT_TRUE(sm != nullptr);
    ASSERT_EQ(raftlogString(sm->Get()->GetRaftLog()), want_log);
  }
}

TEST(Raft, SingleNodeCandidate) {
  auto tt = NetWork::New({nullptr});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, SingleNodePreCandidate) {
  auto tt = NetWork::NewWithConfig(preVoteConfig, {nullptr});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, OldMessages) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr});
  // make 0 leader @ term 3
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
  tt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgApp).Term(2).Entries({NEW_ENT().Index(3).Term(2)()})()});
  // commit a new entry
  tt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})()});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({NEW_ENT().Index(0).Term(0)(), NEW_ENT().Index(1).Term(1)(),
                       NEW_ENT().Index(2).Term(2)(), NEW_ENT().Index(3).Term(3)(),
                       NEW_ENT().Index(4).Term(3).Data("somedata")()});
  auto ilog = std::make_shared<craft::RaftLog>(storage);
  ilog->SetCommitted(4);
  ilog->GetUnstable().SetOffset(5);
  auto base = raftlogString(ilog.get());
  for (auto& p : tt->Peers()) {
    auto sm = std::dynamic_pointer_cast<Raft>(p.second);
    ASSERT_TRUE(sm != nullptr);
    ASSERT_EQ(base, raftlogString(sm->Get()->GetRaftLog()));
  }
}

// TestOldMessagesReply - optimization - reply with new term.
TEST(Raft, Propposal) {
  struct Test {
    std::shared_ptr<NetWork> network;
    bool success;
  };
  std::vector<Test> tests = {
    {NetWork::New({nullptr, nullptr, nullptr}), true},
    {NetWork::New({nullptr, nullptr, BlackHole::New()}), true},
    {NetWork::New({nullptr, BlackHole::New(), BlackHole::New()}), false},
    {NetWork::New({nullptr, BlackHole::New(), BlackHole::New(), nullptr}), false},
    {NetWork::New({nullptr, BlackHole::New(), BlackHole::New(), nullptr, nullptr}), true},
  };

  for (size_t i = 0; i < tests.size(); i++) {
    auto& tt = tests[i];
    std::string data = "somedata";

    // promote 1 to become leader
    tt.network->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
    tt.network->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data(data)()})()});

    std::shared_ptr<craft::RaftLog> want_log = craft::RaftLog::New(std::make_shared<craft::MemoryStorage>());
    if (tt.success) {
      auto storage = std::make_shared<craft::MemoryStorage>();
      storage->SetEntries({NEW_ENT().Index(0).Term(0)(), NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1).Data("somedata")()});
      want_log = std::make_shared<craft::RaftLog>(storage);
      want_log->SetCommitted(2);
      want_log->GetUnstable().SetOffset(3);
    }

    auto base = raftlogString(want_log.get());
    for (auto& p : tt.network->Peers()) {
      auto sm = std::dynamic_pointer_cast<Raft>(p.second);
      if (sm) {
        ASSERT_EQ(raftlogString(sm->Get()->GetRaftLog()), base);
      } else {
        std::cout << "#" << i << ": peer " << p.first << " empty log" << std::endl;
      }
    }
    auto sm = std::dynamic_pointer_cast<Raft>(tt.network->Peers()[1]);
    ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(1));
  }
}

TEST(Raft, ProposalByProxy) {
  struct Test {
    std::shared_ptr<NetWork> network;
  };
  std::vector<Test> tests = {
    {NetWork::New({nullptr, nullptr, nullptr})},
    {NetWork::New({nullptr, nullptr, BlackHole::New()})},
  };

  for (size_t i = 0; i < tests.size(); i++) {
    auto& tt = tests[i];
    // promote 1 the leader
    tt.network->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

    // propose via follower
    tt.network->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})()});

    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->SetEntries({NEW_ENT().Index(0).Term(0)(),
                         NEW_ENT().Index(1).Term(1)(),
                         NEW_ENT().Index(2).Term(1).Data("somedata")()});
    auto want_log = std::make_shared<craft::RaftLog>(storage);
    want_log->SetCommitted(2);
    want_log->GetUnstable().SetOffset(3);
    auto base = raftlogString(want_log.get());
    for (auto& p : tt.network->Peers()) {
      auto sm = std::dynamic_pointer_cast<Raft>(p.second);
      if (sm) {
        ASSERT_EQ(raftlogString(sm->Get()->GetRaftLog()), base);
      } else {
        std::cout << "#" << i << ": peer " << p.first << " empty log" << std::endl;
      }
    }
    auto sm = std::dynamic_pointer_cast<Raft>(tt.network->Peers()[1]);
    ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(1));
  }
}

TEST(Raft, Commit) {
  struct Test {
    std::vector<uint64_t> matches;
    craft::EntryPtrs logs;
    uint64_t sm_term;
    uint64_t w;
  };
  std::vector<Test> tests = {
    // single
    {{1}, {NEW_ENT().Index(1).Term(1)()}, 1, 1},
    {{1}, {NEW_ENT().Index(1).Term(1)()}, 2, 0},
    {{2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 2, 2},
    {{1}, {NEW_ENT().Index(1).Term(2)()}, 2, 1},
    // odd
    {{2, 1, 1}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 1, 1},
    {{2, 1, 1}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()}, 2, 0},
    {{2, 1, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 2, 2},
    {{2, 1, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()}, 2, 0},
    // even
    {{2, 1, 1, 1}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 1, 1},
    {{2, 1, 1, 1}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()}, 2, 0},
    {{2, 1, 1, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 1, 1},
    {{2, 1, 1, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()}, 2, 0},
    {{2, 1, 2, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(2)()}, 2, 2},
    {{2, 1, 2, 2}, {NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()}, 2, 0},
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1})});
    storage->Append(tt.logs);
    raftpb::HardState hard_state;
    hard_state.set_term(tt.sm_term);
    storage->SetHardState(hard_state);

    auto sm = newTestRaft(1, 10, 2, storage);
    for (size_t j = 0; j < tt.matches.size(); j++) {
      auto id = j + 1;
      if (id > 1) {
        sm->Get()->ApplyConfChange(makeConfChange(id, raftpb::ConfChangeAddNode));
      }
      auto pr = sm->Get()->GetTracker().GetProgress(id);
      pr->SetMatch(tt.matches[j]);
      pr->SetNext(tt.matches[j]+1);
    }
    sm->Get()->MaybeCommit();
    ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), tt.w);
  }
}

TEST(Raft, PastElectionTimeout) {
  struct Test {
    int64_t elapse;
    double wprobability;
    bool round; 
  };
  std::vector<Test> tests = {
    {5, 0, false},
    {10, 0.1, true},
    {13, 0.4, true},
    {15, 0.6, true},
    {18, 0.9, true},
    {20, 1, false},
  };
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
    sm->Get()->SetElectionElapsed(tt.elapse);
    double c = 0;
    for (int j = 0; j < 10000; j++) {
      sm->Get()->ResetRandomizedElectionTimeout();
      if (sm->Get()->PastElectionTimeout()) {
        c++;
      }
    }

    double got = c / 10000.0;
    if (tt.round) {
      got = std::floor(got*10+0.5) / 10;
    }
    ASSERT_EQ(got, tt.wprobability);
  }
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
TEST(Raft, StepIgnoreOldTermMsg) {
  bool called = false;
  auto fake_step = [&called](craft::MsgPtr m) {
    called = true;
    return craft::Status::OK();
  };
  auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
  sm->Get()->SetStep(std::move(fake_step));
  sm->Get()->SetTerm(2);
  auto m = std::make_shared<raftpb::Message>();
  m->set_type(raftpb::MessageType::MsgApp);
  m->set_term(sm->Get()->Term() - 1);
  sm->Step(m);
  ASSERT_FALSE(called);
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
TEST(Raft, HandleMsgApp) {
  struct Test {
    craft::MsgPtr m;
    uint64_t windex;
    uint64_t wcommit;
    bool wreject;
  };
  std::vector<Test> tests = {
    // Ensure 1
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(3).Index(2).Commit(3)(), 2, 0, true},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(3).Index(3).Commit(3)(), 2, 0, true},
    // Ensure 2
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(1).Index(1).Commit(1)(), 2, 1, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(0).Index(0).Commit(1)
      .Entries({NEW_ENT().Index(1).Term(2)()})(), 1, 1, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(2).Index(2).Commit(3)
      .Entries({NEW_ENT().Index(3).Term(2)(), NEW_ENT().Index(4).Term(2)()})(),
      4, 3, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(2).Index(2).Commit(4)
      .Entries({NEW_ENT().Index(3).Term(2)()})(), 3, 3, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(1).Index(1).Commit(4)
      .Entries({NEW_ENT().Index(2).Term(2)()})(), 2, 2, false},
    // Ensure 3
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(1).LogTerm(1).Index(1).Commit(3)(), 2, 1, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(1).LogTerm(1).Index(1).Commit(3)
      .Entries({NEW_ENT().Index(2).Term(2)()})(), 2, 2, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(2).Index(2).Commit(3)(), 2, 2, false},
    {NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(2).Index(2).Commit(4)(), 2, 2, false}, 
  };

  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1})});
    storage->Append({NEW_ENT().Index(1).Term(1)(),
                     NEW_ENT().Index(2).Term(2)()});
    auto sm = newTestRaft(1, 10, 1, storage);
    sm->Get()->BecomeFollower(2, craft::Raft::kNone);

    sm->Get()->HandleAppendEntries(tt.m);
    ASSERT_EQ(sm->Get()->GetRaftLog()->LastIndex(), tt.windex);
    ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), tt.wcommit);
    auto m = sm->ReadMessages();
    ASSERT_EQ(m.size(), static_cast<size_t>(1));
    ASSERT_EQ(m[0]->reject(), tt.wreject);
  }
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
TEST(Raft, HandleHeartbeat) {
  uint64_t commit = 2;
  struct Test {
    craft::MsgPtr m;
    uint64_t wcommit;
  };
  std::vector<Test> tests = {
    {NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeat).Commit(commit + 1)(), commit + 1},
    {NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeat).Commit(commit - 1)(), commit},  // do not decrease commit
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2})});
    storage->Append({NEW_ENT().Index(1).Term(1)(),
                     NEW_ENT().Index(2).Term(2)(),
                     NEW_ENT().Index(3).Term(3)()});
    auto sm = newTestRaft(1, 5, 1, storage);
    sm->Get()->BecomeFollower(2, 2);
    sm->Get()->GetRaftLog()->CommitTo(commit);
    sm->Get()->HandleHearbeat(tt.m);
    ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), tt.wcommit);
    auto m = sm->ReadMessages();
    ASSERT_EQ(m.size(), static_cast<size_t>(1));
    ASSERT_EQ(m[0]->type(), raftpb::MessageType::MsgHeartbeatResp);
  }
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
TEST(Raft, HandleHeartbeatResp) {
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  storage->Append({NEW_ENT().Index(1).Term(1)(),
                    NEW_ENT().Index(2).Term(2)(),
                    NEW_ENT().Index(3).Term(3)()});
  auto sm = newTestRaft(1, 5, 1, storage);
  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();
  sm->Get()->GetRaftLog()->CommitTo(sm->Get()->GetRaftLog()->LastIndex());

	// A heartbeat response from a node that is behind; re-send MsgApp
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)()});
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);

  // A second heartbeat response generates another MsgApp re-send
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)()});
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);

  // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp)
            .Index(msgs[0]->index() + msgs[0]->entries().size())()});
  // Consume the message sent in response to MsgAppResp
  sm->ReadMessages();

  // A second heartbeat response generates another MsgApp re-send
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)()});
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(0));
}

// TestRaftFreesReadOnlyMem ensures raft will free read request from
// readOnly readIndexQueue and pendingReadIndex map.
// related issue: https://github.com/etcd-io/etcd/issues/7571
TEST(Raft, RaftFreesReadOnlyMem) {
  auto sm = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();
  sm->Get()->GetRaftLog()->CommitTo(sm->Get()->GetRaftLog()->LastIndex());

  std::string ctx = "ctx";

	// leader starts linearizable read request.
	// more info: raft dissertation 6.4, step 2.
  sm->Step(NEW_MSG().From(2).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(ctx)()})());
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgHeartbeat);
  ASSERT_EQ(msgs[0]->context(), ctx);
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetReadIndexQueue().size(), static_cast<size_t>(1));
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetPendingReadIndex().size(), static_cast<size_t>(1));
  ASSERT_TRUE(sm->Get()->GetReadOnly()->GetPendingReadIndex().count(ctx) > static_cast<size_t>(0));

	// heartbeat responses from majority of followers (1 in this case)
	// acknowledge the authority of the leader.
	// more info: raft dissertation 6.4, step 3.
  sm->Step({NEW_MSG().From(2).Type(raftpb::MessageType::MsgHeartbeatResp).Context(ctx)()});
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetReadIndexQueue().size(), static_cast<size_t>(0));
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetPendingReadIndex().size(), static_cast<size_t>(0));
  ASSERT_TRUE(sm->Get()->GetReadOnly()->GetPendingReadIndex().count(ctx) == static_cast<size_t>(0));
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
TEST(Raft, MsgAppRespWaitReset) {
  auto sm = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

	// The new leader has just emitted a new Term 4 entry; consume those messages
	// from the outgoing queue.
  sm->Get()->BcastAppend();
  sm->ReadMessages();

	// Node 2 acks the first entry, making it committed.
  sm->Step(NEW_MSG().From(2).Type(raftpb::MessageType::MsgAppResp).Index(1)());
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(1));
	// Also consume the MsgApp messages that update Commit on the followers.
  sm->ReadMessages();

	// A new command is now proposed on node 1.
  sm->Step(NEW_MSG().From(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})());
	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(msgs[0]->to(), static_cast<uint64_t>(2));
  ASSERT_EQ(msgs[0]->entries().size(), 1);
  ASSERT_EQ(msgs[0]->entries(0).index(), static_cast<uint64_t>(2));

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
  sm->Step(NEW_MSG().From(3).Type(raftpb::MessageType::MsgAppResp).Index(1)());
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(msgs[0]->to(), static_cast<uint64_t>(3));
  ASSERT_EQ(msgs[0]->entries().size(), 1);
  ASSERT_EQ(msgs[0]->entries(0).index(), static_cast<uint64_t>(2));
}

static void testRecvMsgVote(raftpb::MessageType msg_type) {
  struct Test {
    craft::RaftStateType state;
    uint64_t index;
    uint64_t log_term;
    uint64_t vote_for;
    bool wreject;
  };
  std::vector<Test> tests = {
    {craft::RaftStateType::kFollower, 0, 0, craft::Raft::kNone, true},
    {craft::RaftStateType::kFollower, 0, 1, craft::Raft::kNone, true},
    {craft::RaftStateType::kFollower, 0, 2, craft::Raft::kNone, true},
    {craft::RaftStateType::kFollower, 0, 3, craft::Raft::kNone, false},

		{craft::RaftStateType::kFollower, 1, 0, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 1, 1, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 1, 2, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 1, 3, craft::Raft::kNone, false},

		{craft::RaftStateType::kFollower, 2, 0, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 2, 1, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 2, 2, craft::Raft::kNone, false},
		{craft::RaftStateType::kFollower, 2, 3, craft::Raft::kNone, false},

		{craft::RaftStateType::kFollower, 3, 0, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 3, 1, craft::Raft::kNone, true},
		{craft::RaftStateType::kFollower, 3, 2, craft::Raft::kNone, false},
		{craft::RaftStateType::kFollower, 3, 3, craft::Raft::kNone, false},

		{craft::RaftStateType::kFollower, 3, 2, 2, false},
		{craft::RaftStateType::kFollower, 3, 2, 1, true},

		{craft::RaftStateType::kLeader, 3, 3, 1, true},
		{craft::RaftStateType::kPreCandidate, 3, 3, 1, true},
		{craft::RaftStateType::kCandidate, 3, 3, 1, true},
  };
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
    sm->Get()->SetState(tt.state);
    if (tt.state == craft::RaftStateType::kFollower) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepFollower, sm->Get(), std::placeholders::_1));
    } else if (tt.state == craft::RaftStateType::kCandidate || tt.state == craft::RaftStateType::kPreCandidate) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepCandidate, sm->Get(), std::placeholders::_1));
    } else if (tt.state == craft::RaftStateType::kLeader) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepLeader, sm->Get(), std::placeholders::_1));
    }
    sm->Get()->SetVote(tt.vote_for);
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->SetEntries({NEW_ENT()(),
                         NEW_ENT().Index(1).Term(2)(),
                         NEW_ENT().Index(2).Term(2)()});
    auto want_log = std::make_shared<craft::RaftLog>(storage);
    want_log->GetUnstable().SetOffset(3);
    *(sm->Get()->GetRaftLog()) = *want_log;

		// raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
		// test we're only testing MsgVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
    auto term = std::max(sm->Get()->GetRaftLog()->LastTerm(), tt.log_term);
    sm->Get()->SetTerm(term);
    sm->Step(NEW_MSG().Type(msg_type).Term(term).From(2).Index(tt.index).LogTerm(tt.log_term)());

    auto msgs = sm->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    ASSERT_EQ(msgs[0]->type(), craft::Util::VoteRespMsgType(msg_type));
    ASSERT_EQ(msgs[0]->reject(), tt.wreject);
  }
}

TEST(Raft, RecvMsgVote) {
  testRecvMsgVote(raftpb::MessageType::MsgVote);
}

TEST(Raft, RecvMsgPreVote) {
  testRecvMsgVote(raftpb::MessageType::MsgPreVote);
}

TEST(Raft, StateTransition) {
  struct Test {
    craft::RaftStateType from;
    craft::RaftStateType to;
    bool wallow;
    uint64_t wterm;
    uint64_t wlead;
  };
  std::vector<Test> tests = {
		{craft::RaftStateType::kFollower, craft::RaftStateType::kFollower, true, 1, craft::Raft::kNone},
		{craft::RaftStateType::kFollower, craft::RaftStateType::kPreCandidate, true, 0, craft::Raft::kNone},
		{craft::RaftStateType::kFollower, craft::RaftStateType::kCandidate, true, 1, craft::Raft::kNone},
		// {craft::RaftStateType::kFollower, craft::RaftStateType::kLeader, false, 0, craft::Raft::kNone},

		{craft::RaftStateType::kPreCandidate, craft::RaftStateType::kFollower, true, 0, craft::Raft::kNone},
		{craft::RaftStateType::kPreCandidate, craft::RaftStateType::kPreCandidate, true, 0, craft::Raft::kNone},
		{craft::RaftStateType::kPreCandidate, craft::RaftStateType::kCandidate, true, 1, craft::Raft::kNone},
		{craft::RaftStateType::kPreCandidate, craft::RaftStateType::kLeader, true, 0, 1},

		{craft::RaftStateType::kCandidate, craft::RaftStateType::kFollower, true, 0, craft::Raft::kNone},
		{craft::RaftStateType::kCandidate, craft::RaftStateType::kPreCandidate, true, 0, craft::Raft::kNone},
		{craft::RaftStateType::kCandidate, craft::RaftStateType::kCandidate, true, 1, craft::Raft::kNone},
		{craft::RaftStateType::kCandidate, craft::RaftStateType::kLeader, true, 0, 1},

		{craft::RaftStateType::kLeader, craft::RaftStateType::kFollower, true, 1, craft::Raft::kNone},
		// {craft::RaftStateType::kLeader, craft::RaftStateType::kPreCandidate, false, 0, craft::Raft::kNone},
		// {craft::RaftStateType::kLeader, craft::RaftStateType::kCandidate, false, 1, craft::Raft::kNone},
		{craft::RaftStateType::kLeader, craft::RaftStateType::kLeader, true, 0, 1},
  };
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
    sm->Get()->SetState(tt.from);

    if (tt.to == craft::RaftStateType::kFollower) {
      sm->Get()->BecomeFollower(tt.wterm, tt.wlead);
    } else if (tt.to == craft::RaftStateType::kPreCandidate) {
      sm->Get()->BecomePreCandidate();
    } else if (tt.to == craft::RaftStateType::kCandidate) {
      sm->Get()->BecomeCandidate();
    } else if (tt.to == craft::RaftStateType::kLeader) {
      sm->Get()->BecomeLeader();
    }

    ASSERT_EQ(sm->Get()->Term(), tt.wterm);
    ASSERT_EQ(sm->Get()->Lead(), tt.wlead);
  }
}

TEST(Raft, AllServerStepdown) {
  struct Test {
    craft::RaftStateType state;
    craft::RaftStateType wstate;
    uint64_t wterm;
    uint64_t windex;
  };
  std::vector<Test> tests = {
		{craft::RaftStateType::kFollower, craft::RaftStateType::kFollower, 3, 0},
		{craft::RaftStateType::kPreCandidate, craft::RaftStateType::kFollower, 3, 0},
		{craft::RaftStateType::kCandidate, craft::RaftStateType::kFollower, 3, 0},
		{craft::RaftStateType::kLeader, craft::RaftStateType::kFollower, 3, 1},
  };

  std::vector<raftpb::MessageType> tmsg_types = {raftpb::MessageType::MsgVote, raftpb::MessageType::MsgApp};
  uint64_t tterm = 3;
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    if (tt.state == craft::RaftStateType::kFollower) {
      sm->Get()->BecomeFollower(1, craft::Raft::kNone);
    } else if (tt.state == craft::RaftStateType::kPreCandidate) {
      sm->Get()->BecomePreCandidate();
    } else if (tt.state == craft::RaftStateType::kCandidate) {
      sm->Get()->BecomeCandidate();
    } else if (tt.state == craft::RaftStateType::kLeader) {
      sm->Get()->BecomeCandidate();
      sm->Get()->BecomeLeader();
    }

    for (auto msg_type : tmsg_types) {
      sm->Step(NEW_MSG().From(2).Type(msg_type).Term(tterm).LogTerm(tterm)());
      ASSERT_EQ(sm->Get()->State(), tt.wstate);
      ASSERT_EQ(sm->Get()->Term(), tt.wterm);
      ASSERT_EQ(sm->Get()->GetRaftLog()->LastIndex(), tt.windex);
      ASSERT_EQ(sm->Get()->GetRaftLog()->AllEntries().size(), tt.windex);
      uint64_t wlead = 2;
      if (msg_type == raftpb::MessageType::MsgVote) {
        wlead = craft::Raft::kNone;
      }
      ASSERT_EQ(sm->Get()->Lead(), wlead);
    }
  }
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or MsgApp from leader, "Step" resets the term
// with leader's and reverts back to follower.
void testCandidateResetTerm(raftpb::MessageType mt) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  auto nt = NetWork::New({a, b, c});

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(b->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kFollower);

  // isolate 3 and increase term in rest
  nt->Isolate(3);

  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(b->Get()->State(), craft::RaftStateType::kFollower);

	// trigger campaign in isolated c
  c->Get()->ResetRandomizedElectionTimeout();
  for (int64_t i = 0; i < c->Get()->RandomizedElectionTimeout(); i++) {
    c->Get()->Tick();
  }
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kCandidate);

  nt->Recover();

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
  nt->Send({NEW_MSG().From(1).To(3).Term(a->Get()->Term()).Type(mt)()});
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kFollower);

  // follower c term is reset with leader's
  ASSERT_EQ(a->Get()->Term(), c->Get()->Term());
}

TEST(Raft, CandidateResetTermMsgHeartbeat) {
  testCandidateResetTerm(raftpb::MessageType::MsgHeartbeat);
}

TEST(Raft, CandidateResetTermMsgApp) {
  testCandidateResetTerm(raftpb::MessageType::MsgApp);
}

TEST(Raft, LeaderStepdownWhenQuorumActive) {
  auto sm = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  sm->Get()->SetCheckQuorum(true);

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

  for (int64_t i = 0; i < sm->Get()->ElectionTimeout() + 1; i++) {
    sm->Step(NEW_MSG().From(2).Type(raftpb::MessageType::MsgHeartbeatResp).Term(sm->Get()->Term())());
    sm->Get()->Tick();
  }
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, LeaderStepdownWhenQuorumLost) {
  auto sm = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  sm->Get()->SetCheckQuorum(true);

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

  for (int64_t i = 0; i < sm->Get()->ElectionTimeout() + 1; i++) {
    sm->Get()->Tick();
  }
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);
}

TEST(Raft, LeaderSupersedingWithCheckQuorum) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  a->Get()->SetCheckQuorum(true);
  b->Get()->SetCheckQuorum(true);
  c->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({a, b, c});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);
  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(b->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kFollower);

  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

	// Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kCandidate);

	// Letting b's electionElapsed reach to electionTimeout
  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, LeaderElectionWithCheckQuorum) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  a->Get()->SetCheckQuorum(true);
  b->Get()->SetCheckQuorum(true);
  c->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({a, b, c});
  a->Get()->SetRandomizedElectionTimeout(a->Get()->ElectionTimeout()+1);
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout()+2);

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kFollower);

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
  a->Get()->SetRandomizedElectionTimeout(a->Get()->ElectionTimeout()+1);
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout()+2);
  for (int64_t i = 0; i < a->Get()->ElectionTimeout(); i++) {
    a->Get()->Tick();
  }
  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kLeader);
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
TEST(Raft, FreeStuckCandidateWithCheckQuorum) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  a->Get()->SetCheckQuorum(true);
  b->Get()->SetCheckQuorum(true);
  c->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({a, b, c});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);

  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(1);
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_EQ(b->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kCandidate);
  ASSERT_EQ(c->Get()->Term(), b->Get()->Term() + 1);

  // Vote again for safety
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kCandidate);
  ASSERT_EQ(c->Get()->Term(), b->Get()->Term() + 2);

  nt->Recover();
  nt->Send({NEW_MSG().From(1).To(3).Type(raftpb::MessageType::MsgHeartbeat).Term(a->Get()->Term())()});

  // Disrupt the leader so that the stuck peer is freed
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kFollower);

  ASSERT_EQ(c->Get()->Term(), a->Get()->Term());

  // Vote again, should become leader this time
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(c->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, NonPromotableVoterWithCheckQuorum) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1})}));

  a->Get()->SetCheckQuorum(true);
  b->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({a, b});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
  b->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeRemoveNode));
  ASSERT_FALSE(b->Get()->Promotable());

  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(b->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(b->Get()->Lead(), static_cast<uint64_t>(1));
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
TEST(Raft, DisruptiveFollower) {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n3 = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  n1->Get()->SetCheckQuorum(true);
  n2->Get()->SetCheckQuorum(true);
  n3->Get()->SetCheckQuorum(true);

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);
  n3->Get()->BecomeFollower(1, craft::Raft::kNone);

  auto nt = NetWork::New({n1, n2, n3});

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateFollower
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kFollower);  

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
  n3->Get()->SetRandomizedElectionTimeout(n3->Get()->ElectionTimeout() + 2);
  for (int64_t i = 0; i < n3->Get()->RandomizedElectionTimeout()-1; i++) {
    n3->Get()->Tick();
  }

	// ideally, before last election tick elapses,
	// the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
  n3->Get()->Tick();

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateCandidate
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kCandidate);
	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 3
  ASSERT_EQ(n1->Get()->Term(), static_cast<uint64_t>(2));
  ASSERT_EQ(n2->Get()->Term(), static_cast<uint64_t>(2));
  ASSERT_EQ(n3->Get()->Term(), static_cast<uint64_t>(3));

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
  nt->Send({NEW_MSG().From(1).To(3).Term(n1->Get()->Term()).Type(raftpb::MessageType::MsgHeartbeat)()});

	// then candidate n3 responds with "pb.MsgAppResp" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	// n1.state == StateFollower
	// n2.state == StateFollower
	// n3.state == StateCandidate
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kCandidate);
	// check term
	// n1.Term == 3
	// n2.Term == 2
	// n3.Term == 3
  ASSERT_EQ(n1->Get()->Term(), static_cast<uint64_t>(3));
  ASSERT_EQ(n2->Get()->Term(), static_cast<uint64_t>(2));
  ASSERT_EQ(n3->Get()->Term(), static_cast<uint64_t>(3));
}

TEST(Raft, ReadOnlyOptionSafe) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  auto nt = NetWork::New({a, b, c});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);
  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);

  struct Test {
    std::shared_ptr<Raft> sm;
    int proposals;
    uint64_t wri;
    std::string wctx;
  };
  std::vector<Test> tests = {
    {a, 10, 11, "ctx1"},
    {b, 10, 21, "ctx2"},
    {c, 10, 31, "ctx3"},
    {a, 10, 41, "ctx4"},
    {b, 10, 51, "ctx5"},
    {c, 10, 61, "ctx6"},
  };

  for (auto& tt : tests) {
    for (int j = 0; j < tt.proposals; j++) {
      nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
    }

    nt->Send({NEW_MSG().From(tt.sm->Get()->ID()).To(tt.sm->Get()->ID()).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(tt.wctx)()})()});

    auto r = tt.sm;
    ASSERT_NE(r->Get()->GetReadStates().size(), static_cast<size_t>(0));
    auto& rs = r->Get()->GetReadStates()[0];
    ASSERT_EQ(rs.index, tt.wri);
    ASSERT_EQ(rs.request_ctx, tt.wctx);
    r->Get()->ClearReadStates();
  }
}

TEST(Raft, ReadOnlyWithLearner) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  auto nt = NetWork::New({a, b});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);

  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);

  struct Test {
    std::shared_ptr<Raft> sm;
    int proposals;
    uint64_t wri;
    std::string wctx;
  };
  std::vector<Test> tests = {
    {a, 10, 11, "ctx1"},
    {b, 10, 21, "ctx2"},
    {a, 10, 31, "ctx3"},
    {b, 10, 41, "ctx4"},
  };

  for (auto& tt : tests) {
    for (int j = 0; j < tt.proposals; j++) {
      nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
    }

    nt->Send({NEW_MSG().From(tt.sm->Get()->ID()).To(tt.sm->Get()->ID()).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(tt.wctx)()})()});

    auto r = tt.sm;
    ASSERT_NE(r->Get()->GetReadStates().size(), static_cast<size_t>(0));
    auto& rs = r->Get()->GetReadStates()[0];
    ASSERT_EQ(rs.index, tt.wri);
    ASSERT_EQ(rs.request_ctx, tt.wctx);
    r->Get()->ClearReadStates();
  }
}

TEST(Raft, ReadOnlyOptionLease) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  a->Get()->GetReadOnly()->SetOption(craft::ReadOnly::kLeaseBased);
  b->Get()->GetReadOnly()->SetOption(craft::ReadOnly::kLeaseBased);
  c->Get()->GetReadOnly()->SetOption(craft::ReadOnly::kLeaseBased);
  a->Get()->SetCheckQuorum(true);
  b->Get()->SetCheckQuorum(true);
  c->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({a, b, c});
  b->Get()->SetRandomizedElectionTimeout(b->Get()->ElectionTimeout() + 1);
  for (int64_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
    b->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kLeader);

  struct Test {
    std::shared_ptr<Raft> sm;
    int proposals;
    uint64_t wri;
    std::string wctx;
  };
  std::vector<Test> tests = {
		{a, 10, 11, "ctx1"},
		{b, 10, 21, "ctx2"},
		{c, 10, 31, "ctx3"},
		{a, 10, 41, "ctx4"},
		{b, 10, 51, "ctx5"},
		{c, 10, 61, "ctx6"},
  };
  for (auto& tt : tests) {
    for (int j = 0; j < tt.proposals; j++) {
      nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
    }

    nt->Send({NEW_MSG().From(tt.sm->Get()->ID()).To(tt.sm->Get()->ID()).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(tt.wctx)()})()});

    auto r = tt.sm;
    ASSERT_NE(r->Get()->GetReadStates().size(), static_cast<size_t>(0));
    auto& rs = r->Get()->GetReadStates()[0];
    ASSERT_EQ(rs.index, tt.wri);
    ASSERT_EQ(rs.request_ctx, tt.wctx);
    r->Get()->ClearReadStates();
  }
}

// TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
TEST(Raft, ReadOnlyForNewLeader) {
  struct NodeConfig {
    uint64_t id;
    uint64_t committed;
    uint64_t applied;
    uint64_t compact_index;
  };
  std::vector<NodeConfig> node_configs = {
    {1, 1, 1, 0},
    {2, 2, 2, 2},
    {3, 2, 2, 2},
  };
  std::vector<std::shared_ptr<StateMachince>> peers;
  for (auto& c : node_configs) {
    auto storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    storage->Append({NEW_ENT().Index(1).Term(1)(), NEW_ENT().Index(2).Term(1)()});
    raftpb::HardState hs;
    hs.set_term(1);
    hs.set_commit(c.committed);
    storage->SetHardState(hs);
    if (c.compact_index != 0) {
      storage->Compact(c.compact_index);
    }
    auto cfg = newTestConfig(c.id, 10, 1, storage);
    cfg.applied = c.applied;
    peers.emplace_back(newTestRaftWithConfig(cfg));
  }
  auto nt = NetWork::New(peers);

	// Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
  nt->Ignore(raftpb::MessageType::MsgApp);
  // Force peer a to become leader.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  // Ensure peer a drops read only request.
  uint64_t windex = 4;
  std::string wctx = "ctx";
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(wctx)()})()});
  ASSERT_EQ(sm->Get()->GetReadStates().size(), static_cast<size_t>(0));

  nt->Recover();

	// Force peer a to commit a log entry at its term
  for (int64_t i = 0; i < sm->Get()->HeartbeatTimeout(); i++) {
    sm->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), static_cast<uint64_t>(4));
  auto last_log_term = sm->Get()->GetRaftLog()->ZeroTermOnErrCompacted(sm->Get()->GetRaftLog()->Term(sm->Get()->GetRaftLog()->Committed()));
  ASSERT_EQ(last_log_term, sm->Get()->Term());

	// Ensure peer a processed postponed read only request after it committed an entry at its term.
  ASSERT_EQ(sm->Get()->GetReadStates().size(), static_cast<size_t>(1));
  auto& rs = sm->Get()->GetReadStates()[0];
  ASSERT_EQ(rs.index, windex);
  ASSERT_EQ(rs.request_ctx, wctx);

	// Ensure peer a accepts read only request after it committed an entry at its term.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(wctx)()})()});
  ASSERT_EQ(sm->Get()->GetReadStates().size(), static_cast<size_t>(2));
  auto& rs2 = sm->Get()->GetReadStates()[1];
  ASSERT_EQ(rs2.index, windex);
  ASSERT_EQ(rs2.request_ctx, wctx);
}

TEST(Raft, LeaderAppResp) {
	// initial progress: match = 0; next = 3
  struct Test {
    uint64_t index;
    bool reject;
    // progress
    uint64_t wmatch;
    uint64_t wnext;
    // message
    size_t wmsg_num;
    uint64_t windex;
    uint64_t wcommitted;
  };
  std::vector<Test> tests = {
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit; decrease next and send probing msg
		{2, false, 2, 4, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		{0, false, 0, 3, 0, 0, 0}, // ignore heartbeat replies
  };
  for (auto& tt : tests) {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->SetEntries({NEW_ENT()(), NEW_ENT().Index(1).Term(0)(), NEW_ENT().Index(2).Term(1)()});
    auto wlog = std::make_shared<craft::RaftLog>(storage);
    wlog->GetUnstable().SetOffset(3);
    *sm->Get()->GetRaftLog() = *wlog;

    sm->Get()->BecomeCandidate();
    sm->Get()->BecomeLeader();
    sm->ReadMessages();

    sm->Step(NEW_MSG()
                 .From(2)
                 .Type(raftpb::MessageType::MsgAppResp)
                 .Index(tt.index)
                 .Term(sm->Get()->Term())
                 .Reject(tt.reject)
                 .RejectHint(tt.index)());

    auto p = sm->Get()->GetTracker().GetProgress(2);
    ASSERT_EQ(p->Match(), tt.wmatch);
    ASSERT_EQ(p->Next(), tt.wnext);

    auto msgs = sm->ReadMessages();
    ASSERT_EQ(msgs.size(), tt.wmsg_num);
    for (auto msg : msgs) {
      ASSERT_EQ(msg->index(), tt.windex);
      ASSERT_EQ(msg->commit(), tt.wcommitted);
    }
  }
}

// When the leader receives a heartbeat tick, it should
// send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
TEST(Raft, BcastBeat) {
  uint64_t offset = 1000;
  // make a state machine with log.offset = 1000
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(offset);
  s->mutable_metadata()->set_term(1);
  s->mutable_metadata()->mutable_conf_state()->mutable_voters()->Add(1);
  s->mutable_metadata()->mutable_conf_state()->mutable_voters()->Add(2);
  s->mutable_metadata()->mutable_conf_state()->mutable_voters()->Add(3);
  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->ApplySnapshot(s);
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->SetTerm(1);

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(sm->Get()->AppendEntry({NEW_ENT().Index(i+1)()}));
  }
  // slow follower
  sm->Get()->GetTracker().GetProgress(2)->SetMatch(5);
  sm->Get()->GetTracker().GetProgress(2)->SetNext(6);
  // normal follower
  sm->Get()->GetTracker().GetProgress(3)->SetMatch(sm->Get()->GetRaftLog()->LastIndex());
  sm->Get()->GetTracker().GetProgress(3)->SetNext(sm->Get()->GetRaftLog()->LastIndex() + 1);

  sm->Step(NEW_MSG().Type(raftpb::MessageType::MsgBeat)());
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(2));
  std::map<uint64_t, uint64_t> want_commit_map = {
    {2, std::min(sm->Get()->GetRaftLog()->Committed(), sm->Get()->GetTracker().GetProgress(2)->Match())},
    {3, std::min(sm->Get()->GetRaftLog()->Committed(), sm->Get()->GetTracker().GetProgress(3)->Match())},
  };

  for (auto m : msgs) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgHeartbeat);
    ASSERT_EQ(m->index(), static_cast<uint64_t>(0));
    ASSERT_EQ(m->logterm(), static_cast<uint64_t>(0));
    ASSERT_EQ(want_commit_map[m->to()], m->commit());
    want_commit_map.erase(m->to());
    ASSERT_EQ(m->entries().size(), 0);
  }
}

// tests the output of the state machine when receiving MsgBeat
TEST(Raft, RecvMsgBeat) {
  struct Test {
    craft::RaftStateType state;
    size_t wmsg;
  };
  std::vector<Test> tests = {
		{craft::RaftStateType::kLeader, 2},
		// candidate and follower should ignore MsgBeat
		{craft::RaftStateType::kCandidate, 0},
		{craft::RaftStateType::kFollower, 0},
  };
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    auto storage = std::make_shared<craft::MemoryStorage>();
    storage->SetEntries({NEW_ENT()(), NEW_ENT().Index(1).Term(0)(), NEW_ENT().Index(2).Term(1)()});
    auto wlog = std::make_shared<craft::RaftLog>(storage);
    *sm->Get()->GetRaftLog() = *wlog;
    sm->Get()->SetTerm(1);
    sm->Get()->SetState(tt.state);
    if (tt.state == craft::RaftStateType::kFollower) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepFollower, sm->Get(), std::placeholders::_1));
    } else if (tt.state == craft::RaftStateType::kCandidate) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepCandidate, sm->Get(), std::placeholders::_1));
    } else if (tt.state == craft::RaftStateType::kLeader) {
      sm->Get()->SetStep(std::bind(&craft::Raft::StepLeader, sm->Get(), std::placeholders::_1));
    }
    sm->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)());

    auto msgs = sm->ReadMessages();
    ASSERT_EQ(msgs.size(), tt.wmsg);
    for (auto m : msgs) {
      ASSERT_EQ(m->type(), raftpb::MessageType::MsgHeartbeat);
    }
  }
}

TEST(Raft, LeaderIncreaseNext) {
  craft::EntryPtrs previous_ents = {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)()};
  struct Test {
    // progress
    craft::StateType state;
    uint64_t next;

    uint64_t wnext;
  };
  std::vector<Test> tests = {
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
    {craft::StateType::kReplicate, 2, previous_ents.size() + 1 + 1 + 1},
    // state probe, not optimistically increase next
    {craft::StateType::kProbe, 2, 2},
  };
  for (auto& tt : tests) {
    auto sm = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
    sm->Get()->GetRaftLog()->Append(previous_ents);
    sm->Get()->BecomeCandidate();
    sm->Get()->BecomeLeader();
    sm->Get()->GetTracker().GetProgress(2)->SetState(tt.state);
    sm->Get()->GetTracker().GetProgress(2)->SetNext(tt.next);
    sm->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());

    auto p = sm->Get()->GetTracker().GetProgress(2);
    ASSERT_EQ(p->Next(), tt.wnext);
  }
}

TEST(Raft, SendAppendForProgressProbe) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->ReadMessages();
  r->Get()->GetTracker().GetProgress(2)->BecomeProbe();

  // each round is a heartbeat
  for (int i = 0; i < 3; i++) {
    if (i == 0) {
			// we expect that raft will only send out one msgAPP on the first
			// loop. After that, the follower is paused until a heartbeat response is
			// received.
      r->Get()->AppendEntry({NEW_ENT().Data("somedata")()});
      r->Get()->SendAppend(2);
      auto msg = r->ReadMessages();
      ASSERT_EQ(msg.size(), static_cast<size_t>(1));
      ASSERT_EQ(msg[0]->index(), static_cast<uint64_t>(0));
    }

    ASSERT_EQ(r->Get()->GetTracker().GetProgress(2)->ProbeSent(), true);
    for (int j = 0; j < 10; j++) {
      r->Get()->AppendEntry({NEW_ENT().Data("somedata")()});
      r->Get()->SendAppend(2);
      ASSERT_EQ(r->ReadMessages().size(), static_cast<size_t>(0));
    }

    // do  a heartbeat
    for (int j = 0; j < r->Get()->HeartbeatTimeout(); j++) {
      r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)());
    }
    ASSERT_EQ(r->Get()->GetTracker().GetProgress(2)->ProbeSent(), true);

    // consume the heartbeat
    auto msg = r->ReadMessages();
    ASSERT_EQ(msg.size(), static_cast<size_t>(1));
    ASSERT_EQ(msg[0]->type(), raftpb::MessageType::MsgHeartbeat);
  }

	// a heartbeat response will allow another message to be sent
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)());
  auto msg = r->ReadMessages();
  ASSERT_EQ(msg.size(), static_cast<size_t>(1));
  ASSERT_EQ(msg[0]->index(), static_cast<uint64_t>(0));
  ASSERT_EQ(r->Get()->GetTracker().GetProgress(2)->ProbeSent(), true);
}

TEST(Raft, SendAppendForProgressReplicate) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->ReadMessages();
  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();

  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(r->Get()->AppendEntry({NEW_ENT().Data("somedata")()}));
    r->Get()->SendAppend(2);
    auto msgs = r->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  }
}

TEST(Raft, SendAppendForProgressSnapshot) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->ReadMessages();
  r->Get()->GetTracker().GetProgress(2)->BecomeSnapshot(10);

  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(r->Get()->AppendEntry({NEW_ENT().Data("somedata")()}));
    r->Get()->SendAppend(2);
    auto msgs = r->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(0));
  }
}

TEST(Raft, RecvMsgUnreachable) {
  craft::EntryPtrs previous_ents = {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)()};
  auto s = newTestMemoryStorage({withPeers({1, 2})});
  s->Append(previous_ents);
  auto r = newTestRaft(1, 10, 1, s);
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->ReadMessages();
  // set node 2 to state replicate
  r->Get()->GetTracker().GetProgress(2)->SetMatch(3);
  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();
  r->Get()->GetTracker().GetProgress(2)->OptimisticUpdate(5);

  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgUnreachable)());
  ASSERT_EQ(r->Get()->GetTracker().GetProgress(2)->State(), craft::StateType::kProbe);
  ASSERT_EQ(r->Get()->GetTracker().GetProgress(2)->Next(), r->Get()->GetTracker().GetProgress(2)->Match()+1);
}

TEST(Raft, Restore) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  s->mutable_metadata()->mutable_conf_state()->add_voters(3);

  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  ASSERT_TRUE(sm->Get()->Restore(s));

  ASSERT_EQ(sm->Get()->GetRaftLog()->LastIndex(), s->metadata().index());
  auto [term, s1] = sm->Get()->GetRaftLog()->Term(s->metadata().index());
  ASSERT_TRUE(s1.IsOK());
  ASSERT_EQ(term, s->metadata().term());

  auto sg = sm->Get()->GetTracker().VoterNodes();
  std::vector<uint64_t> wvoters = {1, 2, 3};
  ASSERT_EQ(sg, wvoters);

  ASSERT_FALSE(sm->Get()->Restore(s));
  // It should not campaign before actually applying data.
  for (int64_t i = 0; i < sm->Get()->RandomizedElectionTimeout(); i++) {
    sm->Get()->Tick();
  }

  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);
}

// TestRestoreWithLearner restores a snapshot which contains learners.
TEST(Raft, RestoreWithLearner) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  s->mutable_metadata()->mutable_conf_state()->add_learners(3);

  auto storage = newTestMemoryStorage({withPeers({1, 2}), withLearners({3})});
  auto sm = newTestLearnerRaft(3, 8, 2, storage);
  ASSERT_TRUE(sm->Get()->Restore(s));

  ASSERT_EQ(sm->Get()->GetRaftLog()->LastIndex(), s->metadata().index());
  auto [term, s1] = sm->Get()->GetRaftLog()->Term(s->metadata().index());
  ASSERT_TRUE(s1.IsOK());
  ASSERT_EQ(term, s->metadata().term());

  auto sg = sm->Get()->GetTracker().VoterNodes();
  std::vector<uint64_t> wvoters = {1, 2};
  ASSERT_EQ(sg, wvoters);

  auto lns = sm->Get()->GetTracker().LearnerNodes();
  std::vector<uint64_t> wlearners = {3};
  ASSERT_EQ(lns, wlearners);

  for (auto n : wvoters) {
    ASSERT_FALSE(sm->Get()->GetTracker().GetProgress(n)->IsLearner());
  }

  for (auto n : wlearners) {
    ASSERT_TRUE(sm->Get()->GetTracker().GetProgress(n)->IsLearner());
  }

  ASSERT_FALSE(sm->Get()->Restore(s));
}

// Tests if outgoing voter can receive and apply snapshot correctly.
TEST(Raft, RestoreWithVotersOutgoing) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  s->mutable_metadata()->mutable_conf_state()->add_voters(3);
  s->mutable_metadata()->mutable_conf_state()->add_voters(4);
  s->mutable_metadata()->mutable_conf_state()->add_voters_outgoing(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters_outgoing(2);
  s->mutable_metadata()->mutable_conf_state()->add_voters_outgoing(3);

  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  ASSERT_TRUE(sm->Get()->Restore(s));

  ASSERT_EQ(sm->Get()->GetRaftLog()->LastIndex(), s->metadata().index());
  auto [term, s1] = sm->Get()->GetRaftLog()->Term(s->metadata().index());
  ASSERT_TRUE(s1.IsOK());
  ASSERT_EQ(term, s->metadata().term());

  auto sg = sm->Get()->GetTracker().VoterNodes();
  std::vector<uint64_t> wvoters = {1, 2, 3, 4};
  ASSERT_EQ(sg, wvoters);

  ASSERT_FALSE(sm->Get()->Restore(s));
	// It should not campaign before actually applying data.
  for (int64_t i = 0; i < sm->Get()->RandomizedElectionTimeout(); i++) {
    sm->Get()->Tick();
  }
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);
}

// TestRestoreVoterToLearner verifies that a normal peer can be downgraded to a
// learner through a snapshot. At the time of writing, we don't allow
// configuration changes to do this directly, but note that the snapshot may
// compress multiple changes to the configuration into one: the voter could have
// been removed, then readded as a learner and the snapshot reflects both
// changes. In that case, a voter receives a snapshot telling it that it is now
// a learner. In fact, the node has to accept that snapshot, or it is
// permanently cut off from the Raft log.
TEST(Raft, RestoreVoterToLearner) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  s->mutable_metadata()->mutable_conf_state()->add_learners(3);

  auto storage = newTestMemoryStorage({withPeers({1, 2, 3})});
  auto sm = newTestRaft(3, 10, 1, storage);

  ASSERT_FALSE(sm->Get()->IsLearner());
  ASSERT_TRUE(sm->Get()->Restore(s));
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
TEST(Raft, RestoreLearnerPromotion) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  s->mutable_metadata()->mutable_conf_state()->add_voters(3);

  auto storage = newTestMemoryStorage({withPeers({1, 2}), withLearners({3})});
  auto sm = newTestLearnerRaft(3, 10, 1, storage);
  ASSERT_TRUE(sm->Get()->IsLearner());
  ASSERT_TRUE(sm->Get()->Restore(s));
  ASSERT_FALSE(sm->Get()->IsLearner());
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
TEST(Raft, LearnerReceiveSnapshot) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_learners(2);

  auto storage = newTestMemoryStorage({withPeers({1}), withLearners({2})});
  auto n1 = newTestRaft(1, 10, 1, storage);
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));

  n1->Get()->Restore(s);
  auto ready = craft::NewReady(n1->Get(), craft::SoftState{}, raftpb::HardState{});
  storage->ApplySnapshot(ready.snapshot);
  n1->Get()->Advance(ready);

	// Force set n1 appplied index.
  n1->Get()->GetRaftLog()->AppliedTo(n1->Get()->GetRaftLog()->Committed());

  auto nt = NetWork::New({n1, n2});

  n1->Get()->SetRandomizedElectionTimeout(n1->Get()->ElectionTimeout());
  for (int64_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)()});
  ASSERT_EQ(n2->Get()->GetRaftLog()->Committed(), n1->Get()->GetRaftLog()->Committed());
}

TEST(Raft, RestoreIgnoreSnapshot) {
  craft::EntryPtrs previous_ents = {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)()};
  uint64_t commit = 1;
  auto storage = newTestMemoryStorage({withPeers({1, 2})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->GetRaftLog()->Append(previous_ents);
  sm->Get()->GetRaftLog()->CommitTo(commit);

  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(commit);
  s->mutable_metadata()->set_term(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);

  // ignore snapshot
  ASSERT_FALSE(sm->Get()->Restore(s));
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), commit);

  // ignore snapshot and fast forward commit
  s->mutable_metadata()->set_index(commit + 1);
  ASSERT_FALSE(sm->Get()->Restore(s));
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), commit + 1);
}

TEST(Raft, ProvideSnap) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  auto storage = newTestMemoryStorage({withPeers({1})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(s);

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

	// force set the next of node 2, so that node 2 needs a snapshot
  sm->Get()->GetTracker().GetProgress(2)->SetNext(sm->Get()->GetRaftLog()->FirstIndex());
  sm->Get()->Step(NEW_MSG()
                      .From(2)
                      .To(1)
                      .Type(raftpb::MessageType::MsgAppResp)
                      .Index(sm->Get()->GetTracker().GetProgress(2)->Next() - 1)
                      .Reject(true)());

  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgSnap);
}

TEST(Raft, IgnoreProvidingSnap) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  auto storage = newTestMemoryStorage({withPeers({1})});
  auto sm = newTestRaft(1, 10, 1, storage);
  sm->Get()->Restore(s);

  sm->Get()->BecomeCandidate();
  sm->Get()->BecomeLeader();

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
  sm->Get()->GetTracker().GetProgress(2)->SetNext(sm->Get()->GetRaftLog()->FirstIndex()-1);
  sm->Get()->GetTracker().GetProgress(2)->SetRecentActive(false);

  sm->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("somedata")()})());
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), static_cast<size_t>(0));
}

TEST(Raft, RestoreFromSnapMsg) {
  auto s = std::make_shared<raftpb::Snapshot>();
  s->mutable_metadata()->set_index(11);  // magic number
  s->mutable_metadata()->set_term(11);  // magic number
  s->mutable_metadata()->mutable_conf_state()->add_voters(1);
  s->mutable_metadata()->mutable_conf_state()->add_voters(2);
  auto m = NEW_MSG().Type(raftpb::MessageType::MsgSnap).From(1).Term(2).Snapshot(s)();

  auto sm = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  sm->Step(m);
  ASSERT_EQ(sm->Get()->Lead(), static_cast<uint64_t>(1));
}

TEST(Raft, SlowNodeRestore) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);
  for (int j = 0; j <= 100; j++) {
    nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
  }
  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  nextEnts(lead->Get(), nt->Storages()[1]);
  raftpb::ConfState cs;
  for (auto n : lead->Get()->GetTracker().VoterNodes()) {
    cs.add_voters(n);
  }
  nt->Storages()[1]->CreateSnapshot(lead->Get()->GetRaftLog()->Applied(), &cs, "");
  nt->Storages()[1]->Compact(lead->Get()->GetRaftLog()->Applied());

  nt->Recover();
	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
  while (1) {
    nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgBeat)()});
    if (lead->Get()->GetTracker().GetProgress(3)->RecentActive()) {
      break;
    }
  }

  // trigger a snapshot
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  auto follower = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);

  // trigger a commit
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
  ASSERT_EQ(follower->Get()->GetRaftLog()->Committed(), lead->Get()->GetRaftLog()->Committed());
}

// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
TEST(Raft, StepConfig) {
  // a raft that cannot make progress
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  auto index = r->Get()->GetRaftLog()->LastIndex();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Type(raftpb::EntryType::EntryConfChange)()})());
  ASSERT_EQ(r->Get()->GetRaftLog()->LastIndex(), index + 1);
  ASSERT_EQ(r->Get()->PendingConfIndex(), index + 1);
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
TEST(Raft, StepIgnoreConfig) {
	// a raft that cannot make progress
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Type(raftpb::EntryType::EntryConfChange)()})());
  auto index = r->Get()->GetRaftLog()->LastIndex();
  auto pending_conf_index = r->Get()->PendingConfIndex();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Type(raftpb::EntryType::EntryConfChange)()})());
  auto [ents, status] = r->Get()->GetRaftLog()->Entries(index+1, craft::Raft::kNoLimit);
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(ents.size(), static_cast<size_t>(1));
  ASSERT_EQ(ents[0]->type(), raftpb::EntryType::EntryNormal);
  ASSERT_EQ(ents[0]->term(), static_cast<uint64_t>(1));
  ASSERT_EQ(ents[0]->index(), static_cast<uint64_t>(3));
  ASSERT_TRUE(ents[0]->data().empty());
  ASSERT_EQ(r->Get()->PendingConfIndex(), pending_conf_index);
}

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
TEST(Raft, NewLeaderPendingConfig) {
  struct Test {
    bool add_entry;
    uint64_t wpending_index;
  };
  std::vector<Test> tests = {
    {false, 0},
    {true, 1},
  };
  for (auto& tt : tests) {
    auto r= newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
    if (tt.add_entry) {
      ASSERT_TRUE(r->Get()->AppendEntry({NEW_ENT().Type(raftpb::EntryType::EntryNormal)()}));
    }
    r->Get()->BecomeCandidate();
    r->Get()->BecomeLeader();
    ASSERT_EQ(r->Get()->PendingConfIndex(), tt.wpending_index);
  }
}

// TestAddNode tests that addNode could update nodes correctly.
TEST(Raft, AddNode) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  auto nodes = r->Get()->GetTracker().VoterNodes();
  std::vector<uint64_t> wnodes = {1, 2};
  ASSERT_EQ(nodes, wnodes);
}

// TestAddLearner tests that addLearner could update nodes correctly.
TEST(Raft, AddLearner) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
  // Add new learner peer.
  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddLearnerNode));
  ASSERT_FALSE(r->Get()->IsLearner());
  auto nodes = r->Get()->GetTracker().LearnerNodes();
  std::vector<uint64_t> wnodes = {2};
  ASSERT_EQ(nodes, wnodes);
  ASSERT_TRUE(r->Get()->GetTracker().GetProgress(2)->IsLearner());

	// Promote peer to voter.
  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  ASSERT_FALSE(r->Get()->GetTracker().GetProgress(2)->IsLearner());

  // Demote r.
  r->Get()->ApplyConfChange(makeConfChange(1, raftpb::ConfChangeType::ConfChangeAddLearnerNode));
  ASSERT_TRUE(r->Get()->GetTracker().GetProgress(1)->IsLearner());
  ASSERT_TRUE(r->Get()->IsLearner());

  // Promote r again.
  r->Get()->ApplyConfChange(makeConfChange(1, raftpb::ConfChangeType::ConfChangeAddNode));
  ASSERT_FALSE(r->Get()->GetTracker().GetProgress(1)->IsLearner());
  ASSERT_FALSE(r->Get()->IsLearner());
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
TEST(Raft, AddNodeCheckQuorum) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
  r->Get()->SetCheckQuorum(true);

  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  for (int64_t i = 0; i < r->Get()->ElectionTimeout() - 1; i++) {
    r->Get()->Tick();
  }

  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));

	// This tick will reach electionTimeout, which triggers a quorum check.
  r->Get()->Tick();

	// Node 1 should still be the leader after a single tick.
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kLeader);

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
  for (int64_t i = 0; i < r->Get()->ElectionTimeout(); i++) {
    r->Get()->Tick();
  }
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
}

// TestRemoveNode tests that removeNode could update nodes and
// removed list correctly.
TEST(Raft, RemoveNode) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeRemoveNode));
  std::vector<uint64_t> w = {1};
  ASSERT_EQ(r->Get()->GetTracker().VoterNodes(), w);

	// Removing the remaining voter will panic.
  // r->Get()->ApplyConfChange(makeConfChange(1, raftpb::ConfChangeType::ConfChangeRemoveNode));
}

// TestRemoveLearner tests that removeNode could update nodes and
// removed list correctly.
TEST(Raft, RemoveLearner) {
  auto r= newTestLearnerRaft(1, 10, 1, newTestMemoryStorage({withPeers({1}), withLearners({2})}));
  r->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeRemoveNode));
  std::vector<uint64_t> w = {1};
  ASSERT_EQ(r->Get()->GetTracker().VoterNodes(), w);

  w = {};
  ASSERT_EQ(r->Get()->GetTracker().LearnerNodes(), w);

	// Removing the remaining voter will panic.
  // r->Get()->ApplyConfChange(makeConfChange(1, raftpb::ConfChangeType::ConfChangeRemoveNode));
}

TEST(Raft, Promotable) {
  uint64_t id = 1;
  struct Test {
    std::vector<uint64_t> peers;
    bool wp;
  };
  std::vector<Test> tests = {
    {{1}, true},
    {{1, 2, 3}, true},
    {{}, false},
    {{2, 3}, false},
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(id, 5, 1, newTestMemoryStorage({withPeers(tt.peers)}));
    ASSERT_EQ(r->Get()->Promotable(), tt.wp);
  }
}

TEST(Raft, RaftNodes) {
  struct Test {
    std::vector<uint64_t> ids;
    std::vector<uint64_t> wids;
  };
  std::vector<Test> tests = {
    {
      {1, 2, 3},
      {1, 2, 3},
    },
    {
      {3, 2, 1},
      {1, 2, 3},
    }
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers(tt.ids)}));
    ASSERT_EQ(r->Get()->GetTracker().VoterNodes(), tt.wids);
  }
}

static void testCampaignWhileLeader(bool pre_vote) {
  auto cfg = newTestConfig(1, 5, 1, newTestMemoryStorage({withPeers({1})}));
  cfg.pre_vote = pre_vote;
  auto r = newTestRaftWithConfig(cfg);
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
	// We don't call campaign() directly because it comes after the check
	// for our current state.
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)());
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kLeader);
  auto term = r->Get()->Term();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)());
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(r->Get()->Term(), term);
}

TEST(Raft, CampaignWhileLeader) {
  testCampaignWhileLeader(false);
}

TEST(Raft, PreCampaignWhileLeader) {
  testCampaignWhileLeader(true);
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
TEST(Raft, CommitAfterRemoveNode) {
  // Create a cluster with two nodes.
  auto s = newTestMemoryStorage({withPeers({1, 2})});
  auto r = newTestRaft(1, 5, 1, s);
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();

  // Begin to remove the second node.
  raftpb::ConfChange cc;
  cc.set_type(raftpb::ConfChangeType::ConfChangeRemoveNode);
  cc.set_node_id(2);
  auto cc_data = cc.SerializeAsString();
  r->Step(NEW_MSG().Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Type(raftpb::EntryType::EntryConfChange).Data(cc_data)()})());

	// Stabilize the log and make sure nothing is committed yet.
  auto ents = nextEnts(r->Get(), s);
  ASSERT_EQ(ents.size(), static_cast<size_t>(0));
  auto cc_index = r->Get()->GetRaftLog()->LastIndex();

	// While the config change is pending, make another proposal.
  r->Step(NEW_MSG().Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Type(raftpb::EntryType::EntryNormal).Data("hello")()})());

	// Node 2 acknowledges the config change, committing it.
  r->Step(NEW_MSG().Type(raftpb::MessageType::MsgAppResp).From(2).Index(cc_index)());
  ents = nextEnts(r->Get(), s);
  ASSERT_EQ(ents.size(), static_cast<size_t>(2));
  ASSERT_EQ(ents[0]->type(), raftpb::EntryType::EntryNormal);
  ASSERT_EQ(ents[0]->data().empty(), true);
  ASSERT_EQ(ents[1]->type(), raftpb::EntryType::EntryConfChange);

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
  r->Get()->ApplyConfChange(craft::ConfChangeI(cc).AsV2());
  ents = nextEnts(r->Get(), s);
  ASSERT_EQ(ents.size(), static_cast<size_t>(1));
  ASSERT_EQ(ents[0]->type(), raftpb::EntryType::EntryNormal);
  ASSERT_EQ(ents[0]->data(), "hello");
}

static void checkLeaderTransferState(craft::Raft* r, craft::RaftStateType state, uint64_t lead) {
  ASSERT_EQ(r->State(), state);
  ASSERT_EQ(r->Lead(), lead);
  ASSERT_EQ(r->LeadTransferee(), craft::Raft::kNone);
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
TEST(Raft, LeaderTransferToUpToDateNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(lead->Get()->Lead(), static_cast<uint64_t>(1));

  // Transfer leadership to 2.
  nt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});

  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 2);

	// After some log replication, transfer leadership back to 1.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  nt->Send({NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
TEST(Raft, LeaderTransferToUpToDateNodeFromFollower) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(lead->Get()->Lead(), static_cast<uint64_t>(1));

  // Transfer leadership to 2.
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgTransferLeader)()});

  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 2);

	// After some log replication, transfer leadership back to 1.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
TEST(Raft, LeaderTransferWithCheckQuorum) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  for (int i = 1; i < 4; i++) {
    auto r = std::dynamic_pointer_cast<Raft>(nt->Peers()[i]);
    r->Get()->SetCheckQuorum(true);
    r->Get()->SetRandomizedElectionTimeout(r->Get()->ElectionTimeout() + i);
  }

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
  auto f = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  for (int64_t i = 0; i < f->Get()->ElectionTimeout(); i++) {
    f->Get()->Tick();
  }

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(lead->Get()->Lead(), static_cast<uint64_t>(1));

	// Transfer leadership to 2.
  nt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});

  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 2);

  // After some log replication, transfer leadership back to 1.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  nt->Send({NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

TEST(Raft, LeaderTransferToSlowFollower) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  nt->Recover();

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(lead->Get()->GetTracker().GetProgress(3)->Match(), static_cast<uint64_t>(1));

	// Transfer leadership to 3 when node 3 is lack of log.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 3);
}

TEST(Raft, LeaderTransferAfterSnapshot) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  nextEnts(lead->Get(), nt->Storages()[1]);
  raftpb::ConfState cs;
  for (auto n : lead->Get()->GetTracker().VoterNodes()) {
    cs.add_voters(n);
  }
  nt->Storages()[1]->CreateSnapshot(lead->Get()->GetRaftLog()->Applied(), &cs, "");
  nt->Storages()[1]->Compact(lead->Get()->GetRaftLog()->Applied());

  nt->Recover();
  ASSERT_EQ(lead->Get()->GetTracker().GetProgress(3)->Match(), static_cast<uint64_t>(1));

  auto filtered = NEW_MSG()();
  // Snapshot needs to be applied before sending MsgAppResp
  nt->SetMsgHook([&filtered](craft::MsgPtr m) {
    if (m->type() != raftpb::MessageType::MsgAppResp || m->from() != 3 || m->reject()) {
      return true;
    }
    *filtered = *m;
    return false;
  });
	// Transfer leadership to 3 when node 3 is lack of snapshot.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(filtered->entries().size(), 0);

  // Apply snapshot and resume progress
  auto follower = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  auto ready = craft::NewReady(follower->Get(), craft::SoftState{}, raftpb::HardState{});
  nt->Storages()[3]->ApplySnapshot(ready.snapshot);
  follower->Get()->Advance(ready);
  nt->SetMsgHook(nullptr);
  nt->Send({filtered});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 3);
}

TEST(Raft, LeaderTransferToSelf) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

TEST(Raft, LeaderTransferToNonExistingNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

	// Transfer leadership to non-existing node, there will be noop.
  nt->Send({NEW_MSG().From(4).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

TEST(Raft, LeaderTransferTimeout) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

	// Transfer leadership to isolated node, wait for timeout.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));
  for (int64_t i = 0; i < lead->Get()->HeartbeatTimeout(); i++) {
    lead->Get()->Tick();
  }
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  for (int64_t i = 0; i < lead->Get()->ElectionTimeout() - lead->Get()->HeartbeatTimeout(); i++) {
    lead->Get()->Tick();
  }
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

TEST(Raft, LeaderTransferIgnoreProposal) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

  // Transfer leadership to isolated node to let transfer pending, then send proposal.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
  auto status = lead->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})());
  ASSERT_STREQ(status.Str(), craft::kErrProposalDropped);
  ASSERT_EQ(lead->Get()->GetTracker().GetProgress(1)->Match(), static_cast<uint64_t>(1));
}

TEST(Raft, LeaderTransferReceiveHigherTermVote) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

	// The LeadTransferee is removed when leadship transferring.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup).Index(1).Term(2)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 2);
}

TEST(Raft, LeaderTransferRemoveNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Ignore(raftpb::MessageType::MsgTimeoutNow);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

  // The LeadTransferee is removed when leadship transferring.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  lead->Get()->ApplyConfChange(makeConfChange(3, raftpb::ConfChangeType::ConfChangeRemoveNode));
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

TEST(Raft, LeaderTransferDemoteNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Ignore(raftpb::MessageType::MsgTimeoutNow);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

	// The LeadTransferee is demoted when leadship transferring.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  lead->Get()->ApplyConfChange(
      makeConfChange({{3, raftpb::ConfChangeType::ConfChangeRemoveNode},
                      {3, raftpb::ConfChangeType::ConfChangeAddLearnerNode}}));

	// Make the Raft group commit the LeaveJoint entry.
  lead->Get()->ApplyConfChange({});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
TEST(Raft, LeaderTransferBack) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);

  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

	// Transfer leadership back to self.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});

  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
TEST(Raft, LeaderTransferSecondTransferToAnotherNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

	// Transfer leadership to another node.
  nt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kFollower, 2);
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
TEST(Raft, LeaderTransferSecondTransferToSameNode) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  nt->Isolate(3);

  auto lead = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(lead->Get()->LeadTransferee(), static_cast<uint64_t>(3));

  for (int64_t i = 0; i < lead->Get()->HeartbeatTimeout(); i++) {
    lead->Get()->Tick();
  }
	// Second transfer leadership request to the same node.
  nt->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  for (int64_t i = 0; i < lead->Get()->ElectionTimeout() - lead->Get()->HeartbeatTimeout(); i++) {
    lead->Get()->Tick();
  }
  checkLeaderTransferState(lead->Get(), craft::RaftStateType::kLeader, 1);
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
TEST(Raft, TransferNonMember) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({2, 3, 4})}));
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTimeoutNow)());
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgVoteResp)());
  r->Step(NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgVoteResp)());
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
TEST(Raft, NodeWithSmallerTermCanCompleteElection) {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n3 = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);
  n3->Get()->BecomeFollower(1, craft::Raft::kNone);

  n1->Get()->SetPreVote(true);
  n2->Get()->SetPreVote(true);
  n3->Get()->SetPreVote(true);

	// cause a network partition to isolate node 3
  auto nt = NetWork::New({n1, n2, n3});
  nt->Cut(1, 3);
  nt->Cut(2, 3);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kPreCandidate);

  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

	// check whether the term values are expected
	// a.Term == 3
	// b.Term == 3
	// c.Term == 1
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(3));

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(3));

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(1));

	// check state
	// a == follower
	// b == leader
	// c == pre-candidate
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kPreCandidate);

  std::cout << "going to bring back peer 3 and kill peer 2" << std::endl;
	// recover the network then immediately isolate b which is currently
	// the leader, this is to emulate the crash of b.
  nt->Recover();
  nt->Cut(2, 1);
  nt->Cut(2, 3);

	// call for election
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

  auto sma = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  auto smb = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
	// do we have a leader?
  ASSERT_FALSE(sma->Get()->State() != craft::RaftStateType::kLeader && smb->Get()->State() != craft::RaftStateType::kLeader);
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
TEST(Raft, PreVoteWithSplitVote) {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n3 = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);
  n3->Get()->BecomeFollower(1, craft::Raft::kNone);

  n1->Get()->SetPreVote(true);
  n2->Get()->SetPreVote(true);
  n3->Get()->SetPreVote(true);

  auto nt = NetWork::New({n1, n2, n3});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

	// simulate leader down. followers start split vote.
  nt->Isolate(1);
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)(),
            NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

	// check whether the term values are expected
	// n2.Term == 3
	// n3.Term == 3
  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(3));

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(3));

	// check state
	// n2 == candidate
	// n3 == candidate
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kCandidate);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kCandidate);

	// node 2 election timeout first
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

	// check whether the term values are expected
	// n2.Term == 4
	// n3.Term == 4
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(4));

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->Term(), static_cast<uint64_t>(4));

	// check state
	// n2 == leader
	// n3 == follower
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);
}

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate,
// it will checkQuorum correctly.
TEST(Raft, PreVoteWithCheckQuorum) {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n3 = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);
  n3->Get()->BecomeFollower(1, craft::Raft::kNone);

  n1->Get()->SetPreVote(true);
  n2->Get()->SetPreVote(true);
  n3->Get()->SetPreVote(true);

  n1->Get()->SetCheckQuorum(true);
  n2->Get()->SetCheckQuorum(true);
  n3->Get()->SetCheckQuorum(true);

  auto nt = NetWork::New({n1, n2, n3});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

	// isolate node 1. node 2 and node 3 have leader info
  nt->Isolate(1);

	// check state
  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

	// node 2 will ignore node 3's PreVote
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

	// Do we have a leader?
  ASSERT_FALSE(n2->Get()->State() != craft::RaftStateType::kLeader &&
               n3->Get()->State() != craft::RaftStateType::kLeader);
}

// TestLearnerCampaign verifies that a learner won't campaign even if it receives
// a MsgHup or MsgTimeoutNow.
TEST(Raft, LearnerCampaign) {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1})}));
  n1->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddLearnerNode));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1})}));
  n2->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddLearnerNode));
  auto nt = NetWork::New({n1, n2});
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

  ASSERT_TRUE(n2->Get()->IsLearner());
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);

  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n1->Get()->Lead(), static_cast<uint64_t>(1));

	// NB: TransferLeader already checks that the recipient is not a learner, but
	// the check could have happened by the time the recipient becomes a learner,
	// in which case it will receive MsgTimeoutNow as in this test case and we
	// verify that it's ignored.
  nt->Send({NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgTimeoutNow)()});
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
std::shared_ptr<NetWork> newPreVoteMigrationCluster() {
  auto n1 = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n2 = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto n3 = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  n1->Get()->BecomeFollower(1, craft::Raft::kNone);
  n2->Get()->BecomeFollower(1, craft::Raft::kNone);
  n3->Get()->BecomeFollower(1, craft::Raft::kNone);

  n1->Get()->SetPreVote(true);
  n2->Get()->SetPreVote(true);
	// We intentionally do not enable PreVote for n3, this is done so in order
	// to simulate a rolling restart process where it's possible to have a mixed
	// version cluster with replicas with PreVote enabled, and replicas without.

  auto nt = NetWork::New({n1, n2, n3});
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});

	// Cause a network partition to isolate n3.
  nt->Isolate(3);
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateCandidate
  EXPECT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  EXPECT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  EXPECT_EQ(n3->Get()->State(), craft::RaftStateType::kCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 4
  EXPECT_EQ(n1->Get()->Term(), static_cast<uint64_t>(2));
  EXPECT_EQ(n2->Get()->Term(), static_cast<uint64_t>(2));
  EXPECT_EQ(n3->Get()->Term(), static_cast<uint64_t>(4));

	// Enable prevote on n3, then recover the network
  n3->Get()->SetPreVote(true);
  nt->Recover();
  return nt;
}

TEST(Raft, PreVoteMigrationCanCompleteElection) {
  auto nt = newPreVoteMigrationCluster();

  auto n2 = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  auto n3 = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);

  // simulate leader down
  nt->Isolate(1);

	// Call for elections from both n2 and n3.
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

	// check state
	// n2.state == Follower
	// n3.state == PreCandidate
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kPreCandidate);

  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  nt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

	// Do we have a leader?
  ASSERT_FALSE(n2->Get()->State() != craft::RaftStateType::kLeader &&
               n3->Get()->State() != craft::RaftStateType::kLeader);
}

TEST(Raft, PreVoteMigrationWithFreeStuckPreCandidate) {
  auto nt = newPreVoteMigrationCluster();

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
  auto n1 = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  auto n2 = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  auto n3 = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);

  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kPreCandidate);

	// Pre-Vote again for safety
  nt->Send({NEW_MSG().From(3).To(3).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->State(), craft::RaftStateType::kPreCandidate);

  nt->Send({NEW_MSG().From(1).To(3).Type(raftpb::MessageType::MsgHeartbeat).Term(n1->Get()->Term())()});

	// Disrupt the leader so that the stuck peer is freed
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n3->Get()->Term(), n1->Get()->Term());
}

void testConfChangeCheckBeforeCampaign(bool v2) {
  auto nt = NetWork::New({nullptr, nullptr, nullptr});
  auto n1 = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  auto n2 = std::dynamic_pointer_cast<Raft>(nt->Peers()[2]);
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);

	// Begin to remove the third node.
  raftpb::ConfChange cc;
  cc.set_type(raftpb::ConfChangeType::ConfChangeRemoveNode);
  cc.set_node_id(2);
  std::string cc_data;
  raftpb::EntryType ty;
  if (v2) {
    auto ccv2 = craft::ConfChangeI(cc).AsV2();
    cc_data = ccv2.SerializeAsString();
    ty = raftpb::EntryConfChangeV2;
  } else {
    cc_data = cc.SerializeAsString();
    ty = raftpb::EntryConfChange;
  }
  nt->Send({NEW_MSG()
                .From(1)
                .To(1)
                .Type(raftpb::MessageType::MsgProp)
                .Entries({NEW_ENT().Type(ty).Data(cc_data)()})()});

	// Trigger campaign in node 2
  for (int64_t i = 0; i < n2->Get()->RandomizedElectionTimeout(); i++) {
    n2->Get()->Tick();
  }
	// It's still follower because committed conf change is not applied.
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);

	// Transfer leadership to peer 2.
  nt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
	// It's still follower because committed conf change is not applied.
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);
	// Abort transfer leader
  for (int64_t i = 0; i < n1->Get()->RandomizedElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

	// Advance apply
  nextEnts(n2->Get(), nt->Storages()[2]);

	// Transfer leadership to peer 2 again.
  nt->Send({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgTransferLeader)()});
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kLeader);

  nextEnts(n1->Get(), nt->Storages()[1]);
	// Trigger campaign in node 2
  for (int64_t i = 0; i < n1->Get()->RandomizedElectionTimeout(); i++) {
    n1->Get()->Tick();
  }
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kCandidate);
}

// Tests if unapplied ConfChange is checked before campaign.
TEST(Raft, ConfChangeCheckBeforeCampaign) {
  testConfChangeCheckBeforeCampaign(false);
}

// Tests if unapplied ConfChangeV2 is checked before campaign.
TEST(Raft, ConfChangeV2CheckBeforeCampaign) {
  testConfChangeCheckBeforeCampaign(true);
}

TEST(Raft, FastLogRejection) {
  struct Test {
    craft::EntryPtrs leader_log;    // Logs on the leader
    craft::EntryPtrs follower_log;  // Logs on the follower
    uint64_t reject_hint_term;      // Expected term included in rejected MsgAppResp.
    uint64_t reject_hint_index;     // Expected index included in rejected MsgAppResp.
    uint64_t next_append_term;      // Expected term when leader appends after rejected.
    uint64_t next_append_index;     // Expected index when leader appends after rejected.
  };
  std::vector<Test> tests = {
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=7,logTerm=4, entries=...);
		// After rejected leader appends (type=MsgApp,index=3,logTerm=2).
    {
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
        NEW_ENT().Term(4).Index(5)(),
        NEW_ENT().Term(4).Index(6)(),
        NEW_ENT().Term(4).Index(7)(),
      },
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(3).Index(4)(),
        NEW_ENT().Term(3).Index(5)(),
        NEW_ENT().Term(3).Index(6)(),
        NEW_ENT().Term(3).Index(7)(),
        NEW_ENT().Term(3).Index(8)(),
        NEW_ENT().Term(3).Index(9)(),
        NEW_ENT().Term(3).Index(10)(),
        NEW_ENT().Term(3).Index(11)(),
      },
      3, 7, 2, 3,
    },
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=8,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=3).
    {
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(3).Index(4)(),
        NEW_ENT().Term(4).Index(5)(),
        NEW_ENT().Term(4).Index(6)(),
        NEW_ENT().Term(4).Index(7)(),
        NEW_ENT().Term(5).Index(8)(),
      },
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(3).Index(4)(),
        NEW_ENT().Term(3).Index(5)(),
        NEW_ENT().Term(3).Index(6)(),
        NEW_ENT().Term(3).Index(7)(),
        NEW_ENT().Term(3).Index(8)(),
        NEW_ENT().Term(3).Index(9)(),
        NEW_ENT().Term(3).Index(10)(),
        NEW_ENT().Term(3).Index(11)(),
      },
      3, 8, 3, 4,
    },
		// This case is similar to the previous case. However, this time, the
		// leader has a longer uncommitted log tail than the follower.
		// Firstly leader appends (type=MsgApp,index=6,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
    {
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(1).Index(2)(),
        NEW_ENT().Term(1).Index(3)(),
        NEW_ENT().Term(1).Index(4)(),
        NEW_ENT().Term(1).Index(5)(),
        NEW_ENT().Term(1).Index(6)(),
      },
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
      },
      1, 1, 1, 1,
    },
		// This case is similar to the previous case. However, this time, the
		// follower has a longer uncommitted log tail than the leader.
		// Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
    {
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(1).Index(2)(),
        NEW_ENT().Term(1).Index(3)(),
        NEW_ENT().Term(1).Index(4)(),
        NEW_ENT().Term(1).Index(5)(),
        NEW_ENT().Term(1).Index(6)(),
      },
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
        NEW_ENT().Term(4).Index(5)(),
        NEW_ENT().Term(4).Index(6)(),
      },
      1, 1, 1, 1,
    },
		// An normal case that there are no log conflicts.
		// Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=4).
    {
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(1).Index(2)(),
        NEW_ENT().Term(1).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
        NEW_ENT().Term(5).Index(5)(),
      },
      {
        NEW_ENT().Term(1).Index(1)(),
        NEW_ENT().Term(1).Index(2)(),
        NEW_ENT().Term(1).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
      },
      4, 4, 4, 4,
    },
    // Test case from example comment in stepLeader (on leader).
    {
      {
        NEW_ENT().Term(2).Index(1)(),
        NEW_ENT().Term(5).Index(2)(),
        NEW_ENT().Term(5).Index(3)(),
        NEW_ENT().Term(5).Index(4)(),
        NEW_ENT().Term(5).Index(5)(),
        NEW_ENT().Term(5).Index(6)(),
        NEW_ENT().Term(5).Index(7)(),
        NEW_ENT().Term(5).Index(8)(),
        NEW_ENT().Term(5).Index(9)(),
      },
      {
        NEW_ENT().Term(2).Index(1)(),
        NEW_ENT().Term(4).Index(2)(),
        NEW_ENT().Term(4).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
        NEW_ENT().Term(4).Index(5)(),
        NEW_ENT().Term(4).Index(6)(),
      },
      4, 6, 2, 1,
    },
    // Test case from example comment in handleAppendEntries (on follower).
    {
      {
        NEW_ENT().Term(2).Index(1)(),
        NEW_ENT().Term(2).Index(2)(),
        NEW_ENT().Term(2).Index(3)(),
        NEW_ENT().Term(2).Index(4)(),
        NEW_ENT().Term(2).Index(5)(),
      },
      {
        NEW_ENT().Term(2).Index(1)(),
        NEW_ENT().Term(4).Index(2)(),
        NEW_ENT().Term(4).Index(3)(),
        NEW_ENT().Term(4).Index(4)(),
        NEW_ENT().Term(4).Index(5)(),
        NEW_ENT().Term(4).Index(6)(),
        NEW_ENT().Term(4).Index(7)(),
        NEW_ENT().Term(4).Index(8)(),
      },
      2, 1, 2, 1,
    },
  };
  for (auto& tt : tests) {
    auto s1 = std::make_shared<craft::MemoryStorage>();
    s1->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(1);
    s1->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(2);
    s1->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(3);
    s1->Append(tt.leader_log);
    auto s2 = std::make_shared<craft::MemoryStorage>();
    s2->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(1);
    s2->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(2);
    s2->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(3);
    s2->Append(tt.follower_log);

    auto n1 = newTestRaft(1, 10, 1, s1);
    auto n2 = newTestRaft(2, 10, 1, s2);

    n1->Get()->BecomeCandidate();
    n1->Get()->BecomeLeader();

    n2->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHeartbeat)());

    auto msgs = n2->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgHeartbeatResp);
    ASSERT_TRUE(n1->Step(msgs[0]).IsOK());

    msgs = n1->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);

    ASSERT_TRUE(n2->Step(msgs[0]).IsOK());
    msgs = n2->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgAppResp);
    ASSERT_EQ(msgs[0]->reject(), true);
    ASSERT_EQ(msgs[0]->logterm(), tt.reject_hint_term);
    ASSERT_EQ(msgs[0]->rejecthint(), tt.reject_hint_index);

    ASSERT_TRUE(n1->Step(msgs[0]).IsOK());
    msgs = n1->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    ASSERT_EQ(msgs[0]->logterm(), tt.next_append_term);
    ASSERT_EQ(msgs[0]->index(), tt.next_append_index);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}