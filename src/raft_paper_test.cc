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

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
static void testUpdateTermFromMessage(craft::RaftStateType state) {
  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  if (state == craft::RaftStateType::kFollower) {
    r->Get()->BecomeFollower(1, 2);
  } else if (state == craft::RaftStateType::kCandidate) {
    r->Get()->BecomeCandidate();
  } else if (state == craft::RaftStateType::kLeader) {
    r->Get()->BecomeCandidate();
    r->Get()->BecomeLeader();
  }

  r->Step(NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(2)());
  ASSERT_EQ(r->Get()->Term(), static_cast<uint64_t>(2));
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
}

TEST(Raft, FollowerUpdateTermFromMessage) {
  testUpdateTermFromMessage(craft::RaftStateType::kFollower);
}

TEST(Raft, CandidateUpdateTermFromMessage) {
  testUpdateTermFromMessage(craft::RaftStateType::kCandidate);
}

TEST(Raft, LeaderUpdateTermFromMessage) {
  testUpdateTermFromMessage(craft::RaftStateType::kLeader);
}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
TEST(Raft, RejectStaleTermMessage) {
  bool called = false;
  auto fake_step = [&called](craft::MsgPtr m) {
    called = true;
    return craft::Status::OK();
  };

  auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  r->Get()->SetStep(std::move(fake_step));
  raftpb::HardState hs;
  hs.set_term(2);
  r->Get()->LoadState(hs);

  r->Step(NEW_MSG().Type(raftpb::MessageType::MsgApp).Term(r->Get()->Term() - 1)());
  ASSERT_FALSE(called);
}

static craft::MsgPtrs msgsSort(craft::MsgPtrs msgs) {
  std::sort(msgs.begin(), msgs.end(), [](craft::MsgPtr m1, craft::MsgPtr m2) {
    return m1->SerializeAsString() < m2->SerializeAsString();
  });
  return msgs;
}

static void msgsEqual(craft::MsgPtrs msgs1, craft::MsgPtrs msgs2) {
  ASSERT_EQ(msgs1.size(), msgs2.size());
  for (size_t i = 0; i < msgs1.size(); i++) {
    auto msg1 = msgs1[i];
    auto msg2 = msgs2[i];
    ASSERT_EQ(msg1->from(), msg2->from());
    ASSERT_EQ(msg1->to(), msg2->to());
    ASSERT_EQ(msg1->type(), msg2->type());
    ASSERT_EQ(msg1->term(), msg2->term());
    ASSERT_EQ(msg1->logterm(), msg2->logterm());
    ASSERT_EQ(msg1->entries_size(), msg2->entries_size());
  }
}

static void entsEqual(craft::EntryPtrs ents1, craft::EntryPtrs ents2) {
  ASSERT_EQ(ents1.size(), ents2.size());
  for (size_t i = 0; i < ents1.size(); i++) {
    auto ent1 = ents1[i];
    auto ent2 = ents2[i];
    ASSERT_EQ(ent1->index(), ent2->index());
    ASSERT_EQ(ent1->term(), ent2->term());
    ASSERT_EQ(ent1->type(), ent2->type());
    ASSERT_EQ(ent1->data(), ent2->data());
  }
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
TEST(Raft, LeaderBcastBeat) {
	// heartbeat interval
  int64_t hi = 1;
  auto r = newTestRaft(1, 10, hi, newTestMemoryStorage({withPeers({1, 2, 3})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  for (int i = 0; i < 10; i++) {
    r->Get()->AppendEntry({NEW_ENT().Index(i+1)()});
  }

  for (int i = 0; i < hi; i++) {
    r->Get()->Tick();
  }

  auto msgs = r->ReadMessages();
  msgs = msgsSort(msgs);

  craft::MsgPtrs wmsgs = {
    NEW_MSG().From(1).To(2).Term(1).Type(raftpb::MessageType::MsgHeartbeat)(),
    NEW_MSG().From(1).To(3).Term(1).Type(raftpb::MessageType::MsgHeartbeat)(),
  };
  msgsEqual(msgs, wmsgs);
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
static void testNonleaderStartElection(craft::RaftStateType state) {
  // election timeout
  int64_t et = 10;
  auto r = newTestRaft(1, et, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  if (state == craft::RaftStateType::kFollower) {
    r->Get()->BecomeFollower(1, 2);
  } else if (state == craft::RaftStateType::kCandidate) {
    r->Get()->BecomeCandidate();
  }

  for (int64_t i = 1; i < 2 * et; i++) {
    r->Get()->Tick();
  }
  ASSERT_EQ(r->Get()->Term(), static_cast<uint64_t>(2));
  ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kCandidate);
  ASSERT_EQ(r->Get()->GetTracker().IsVote(r->Get()->ID()), true);

  auto msgs = r->ReadMessages();
  msgs = msgsSort(msgs);
  craft::MsgPtrs wmsgs = {
    NEW_MSG().From(1).To(2).Term(2).Type(raftpb::MessageType::MsgVote)(),
    NEW_MSG().From(1).To(3).Term(2).Type(raftpb::MessageType::MsgVote)(),
  };
  msgsEqual(msgs, wmsgs);
}

TEST(Raft, FollowerStartElection) {
  testNonleaderStartElection(craft::RaftStateType::kFollower);
}

TEST(Raft, CandidateStartNewElection) {
  testNonleaderStartElection(craft::RaftStateType::kCandidate);
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
TEST(Raft, LeaderElectionInOneRoundRPC) {
  struct Test {
    int size;
    std::map<uint64_t, bool> votes;
    craft::RaftStateType state;
  };
  std::vector<Test> tests = {
		// win the election when receiving votes from a majority of the servers
    {1, {}, craft::RaftStateType::kLeader},
    {3, {{2, true}, {3, true}}, craft::RaftStateType::kLeader},
    {3, {{2, true}}, craft::RaftStateType::kLeader},
    {5, {{2, true}, {3, true}, {4, true}, {5, true}}, craft::RaftStateType::kLeader},
    {5, {{2, true}, {3, true}, {4, true}}, craft::RaftStateType::kLeader},
    {5, {{2, true}, {3, true}}, craft::RaftStateType::kLeader},

		// return to follower state if it receives vote denial from a majority
    {3, {{2, false}, {3, false}}, craft::RaftStateType::kFollower},
    {5, {{2, false}, {3, false}, {4, false}, {5, false}}, craft::RaftStateType::kFollower},
    {5, {{2, true}, {3, false}, {4, false}, {5, false}}, craft::RaftStateType::kFollower},

		// stay in candidate if it does not obtain the majority
    {3, {}, craft::RaftStateType::kCandidate},
    {5, {{2, true}}, craft::RaftStateType::kCandidate},
    {5, {{2, false}, {3, false}}, craft::RaftStateType::kCandidate},
    {5, {}, craft::RaftStateType::kCandidate},
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers(idsBySize(tt.size))}));

    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)());
    for (auto p : tt.votes) {
      r->Step(NEW_MSG().From(p.first).To(1).Term(r->Get()->Term()).Type(raftpb::MessageType::MsgVoteResp).Reject(!p.second)());
    }
    ASSERT_EQ(r->Get()->State(), tt.state);
    ASSERT_EQ(r->Get()->Term(), static_cast<uint64_t>(1));
  }
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
TEST(Raft, FollowerVote) {
  struct Test {
    uint64_t vote;
    uint64_t nvote;
    bool wreject;
  };
  std::vector<Test> tests = {
    {craft::Raft::kNone, 1, false},
    {craft::Raft::kNone, 2, false},
    {1, 1, false},
    {2, 2, false},
    {1, 2, true},
    {2, 1, true},
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    raftpb::HardState hs;
    hs.set_term(1);
    hs.set_vote(tt.vote);

    r->Step(NEW_MSG().From(tt.nvote).To(1).Term(1).Type(raftpb::MessageType::MsgVote)());

    auto msgs = r->ReadMessages();
    craft::MsgPtrs wmsgs = {
      NEW_MSG().From(1).To(tt.nvote).Term(1).Type(raftpb::MessageType::MsgVoteResp).Reject(tt.wreject)(),
    };
    msgsEqual(msgs, wmsgs);
  }
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
TEST(Raft, CandidateFallback) {
  craft::MsgPtrs tests = {
    NEW_MSG().From(2).To(1).Term(1).Type(raftpb::MessageType::MsgApp)(),
    NEW_MSG().From(2).To(1).Term(2).Type(raftpb::MessageType::MsgApp)(),
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)());
    ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kCandidate);

    r->Step(tt);
    ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
    ASSERT_EQ(r->Get()->Term(), tt->term());
  }
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
static void testNonleaderElectionTimeoutRandomized(craft::RaftStateType state) {
  int64_t et = 10;
  auto r = newTestRaft(1, et, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  std::map<int, bool> timeouts;
  for (int64_t round = 0; round < 50*et; round++) {
    if (state == craft::RaftStateType::kFollower) {
      r->Get()->BecomeFollower(r->Get()->Term()+1, 2);
    } else if (state == craft::RaftStateType::kCandidate) {
      r->Get()->BecomeCandidate();
    }

    int64_t time = 0;
    while (r->ReadMessages().size() == 0) {
      r->Get()->Tick();
      time++;
    }
    timeouts[time] = true;
  }
  for (auto d = et; d < 2*et; d++) {
    ASSERT_TRUE(timeouts[d]);
  }
}

TEST(Raft, FollowerElectionTimeoutRandomized) {
  testNonleaderElectionTimeoutRandomized(craft::RaftStateType::kFollower);
}

TEST(Raft, CandidateElectionTimeoutRandomized) {
  testNonleaderElectionTimeoutRandomized(craft::RaftStateType::kCandidate);
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
void testNonleadersElectionTimeoutNonconflict(craft::RaftStateType state) {
  int64_t et = 10;
  size_t size = 5;
  std::vector<std::shared_ptr<Raft>> rs;
  auto ids = idsBySize(size);
  for (size_t i = 0; i < size; i++) {
    rs.push_back(newTestRaft(ids[i], et, 1, newTestMemoryStorage({withPeers(ids)})));
  }

  int64_t conflicts = 0;
  for (int round = 0; round < 1000; round++) {
    for (auto r : rs) {
      if (state == craft::RaftStateType::kFollower) {
        r->Get()->BecomeFollower(r->Get()->Term()+1, 2);
      } else if (state == craft::RaftStateType::kCandidate) {
        r->Get()->BecomeCandidate();
      }
    }

    size_t timeout_num = 0;
    while (timeout_num == 0) {
      for (auto r : rs) {
        r->Get()->Tick();
        if (r->ReadMessages().size() > 0) {
          timeout_num++;
        }
      }
    }
    // several rafts time out at the same tick
    if (timeout_num > 1) {
      conflicts++;
    }
  }
  ASSERT_TRUE(static_cast<double>(conflicts) / 1000 <= 0.3);
}

TEST(Raft, FollowersElectionTimeoutNonconflict) {
  testNonleadersElectionTimeoutNonconflict(craft::RaftStateType::kFollower);
}

TEST(Raft, CandidatesElectionTimeoutNonconflict) {
  testNonleadersElectionTimeoutNonconflict(craft::RaftStateType::kCandidate);
}

static craft::MsgPtr acceptAndReply(craft::MsgPtr m) {
  EXPECT_EQ(m->type(), raftpb::MessageType::MsgApp);
  return NEW_MSG()
      .From(m->to())
      .To(m->from())
      .Term(m->term())
      .Type(raftpb::MessageType::MsgAppResp)
      .Index(m->index() + m->entries_size())();
}

static void commitNoopEntry(craft::Raft* r, std::shared_ptr<craft::MemoryStorage> s) {
  ASSERT_EQ(r->State(), craft::RaftStateType::kLeader);
  r->BcastAppend();
	// simulate the response of MsgApp
  auto msgs = r->ReadAndClearMsgs();
  for (auto m : msgs) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
    ASSERT_EQ(m->entries_size(), 1);
    ASSERT_EQ(m->entries()[0].data().size(), static_cast<size_t>(0));
    r->Step(acceptAndReply(m));
  }
	// ignore further messages to refresh followers' commit index
  r->ReadAndClearMsgs();
  s->Append(r->GetRaftLog()->UnstableEntries());
  r->GetRaftLog()->AppliedTo(r->GetRaftLog()->Committed());
  r->GetRaftLog()->StableTo(r->GetRaftLog()->LastIndex(), r->GetRaftLog()->LastTerm());
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
TEST(Raft, LeaderStartReplication) {
  auto s = newTestMemoryStorage({withPeers({1, 2, 3})});
  auto r = newTestRaft(1, 10, 1, s);
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  commitNoopEntry(r->Get(), s);
  auto li = r->Get()->GetRaftLog()->LastIndex();

  craft::EntryPtrs ents = {
    NEW_ENT().Data("some data")(),
  };
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries(ents)());
  ASSERT_EQ(r->Get()->GetRaftLog()->LastIndex(), li+1);
  ASSERT_EQ(r->Get()->GetRaftLog()->Committed(), li);
  auto msgs = r->ReadMessages();
  msgsSort(msgs);
  craft::EntryPtrs wents = {
    NEW_ENT().Index(li + 1).Term(1).Data("some data")(),
  };
  craft::MsgPtrs wmsgs = {
    NEW_MSG().From(1).To(2).Term(1).Type(raftpb::MessageType::MsgApp).Index(li).LogTerm(1).Entries(wents).Commit(li)(),
    NEW_MSG().From(1).To(3).Term(1).Type(raftpb::MessageType::MsgApp).Index(li).LogTerm(1).Entries(wents).Commit(li)(),
  };
  msgsEqual(msgs, wmsgs);
  auto g = r->Get()->GetRaftLog()->UnstableEntries();
  entsEqual(g, wents);
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
TEST(Raft, LeaderCommitEntry) {
  auto s = newTestMemoryStorage({withPeers({1, 2, 3})});
  auto r = newTestRaft(1, 10, 1, s);
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  commitNoopEntry(r->Get(), s);
  auto li = r->Get()->GetRaftLog()->LastIndex();
  r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})());

  for (auto m : r->ReadMessages()) {
    r->Step(acceptAndReply(m));
  }

  ASSERT_EQ(r->Get()->GetRaftLog()->Committed(), li + 1);
  craft::EntryPtrs wents = {NEW_ENT().Index(li + 1).Term(1).Data("some data")()};
  auto ents = r->Get()->GetRaftLog()->NextEnts();
  entsEqual(ents, wents);

  auto msgs = r->ReadMessages();
  msgs = msgsSort(msgs);
  for (size_t i = 0; i < msgs.size(); i++) {
    auto m = msgs[i];
    auto w = i + 2;
    ASSERT_EQ(m->to(), w);
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
    ASSERT_EQ(m->commit(), li+1);
  }
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
TEST(Raft, LeaderAcknowledgeCommit) {
  struct Test {
    size_t size;
    std::map<uint64_t, bool> acceptors;
    bool wack;
  };
  std::vector<Test> tests = {
    {1, {}, true},
		{3, {}, false},
		{3, {{2, true}}, true},
		{3, {{2, true}, {3, true}}, true},
		{5, {}, false},
		{5, {{2, true}}, false},
		{5, {{2, true}, {3, true}}, true},
		{5, {{2, true}, {3, true}, {4, true}}, true},
		{5, {{2, true}, {3, true}, {4, true}, {5, true}}, true},
  };
  for (auto& tt : tests) {
    auto s = newTestMemoryStorage({withPeers(idsBySize(tt.size))});
    auto r = newTestRaft(1, 10, 1, s);
    r->Get()->BecomeCandidate();
    r->Get()->BecomeLeader();
    commitNoopEntry(r->Get(), s);
    auto li = r->Get()->GetRaftLog()->LastIndex();
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})());
  
    for (auto m : r->ReadMessages()) {
      if (tt.acceptors[m->to()]) {
        r->Step(acceptAndReply(m));
      }
    }

    auto g = r->Get()->GetRaftLog()->Committed() > li;
    ASSERT_EQ(g, tt.wack);
  }
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
TEST(Raft, LeaderCommitPrecedingEntries) {
  std::vector<craft::EntryPtrs> tests = {
    {},
    {NEW_ENT().Term(2).Index(1)()},
    {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(2).Index(2)()},
    {NEW_ENT().Term(1).Index(1)()},
  };
  for (auto tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    storage->Append(tt);
    auto r = newTestRaft(1, 10, 1, storage);
    raftpb::HardState hs;
    hs.set_term(2);
    r->Get()->LoadState(hs);
    r->Get()->BecomeCandidate();
    r->Get()->BecomeLeader();
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})());

    for (auto m : r->ReadMessages()) {
      r->Step(acceptAndReply(m));
    }

    auto li = tt.size();
    craft::EntryPtrs wents = tt;
    wents.push_back(NEW_ENT().Term(3).Index(li + 1)());
    wents.push_back(NEW_ENT().Term(3).Index(li + 2).Data("some data")());
    auto g = r->Get()->GetRaftLog()->NextEnts();
    entsEqual(g, wents);
  }
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
TEST(Raft, FollowerCommitEntry) {
  struct Test {
    craft::EntryPtrs ents;
    uint64_t commit;
  };
  std::vector<Test> tests = {
    {
      {
        NEW_ENT().Term(1).Index(1).Data("some data")(),
      },
      1,
    },
    {
      {
        NEW_ENT().Term(1).Index(1).Data("some data")(),
        NEW_ENT().Term(1).Index(2).Data("some data2")(),
      },
      2,
    },
    {
      {
        NEW_ENT().Term(1).Index(1).Data("some data2")(),
        NEW_ENT().Term(1).Index(2).Data("some data")(),
      },
      2,
    },
    {
      {
        NEW_ENT().Term(1).Index(1).Data("some data")(),
        NEW_ENT().Term(1).Index(2).Data("some data2")(),
      },
      1,
    },
  };
  for (auto tt : tests) {
    auto r= newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    r->Get()->BecomeFollower(1, 2);

    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgApp).Term(1).Entries(tt.ents).Commit(tt.commit)());

    ASSERT_EQ(r->Get()->GetRaftLog()->Committed(), tt.commit);
    craft::EntryPtrs wents;
    for (size_t i = 0; i < tt.commit; i++) {
      wents.push_back(tt.ents[i]);
    }
    auto g = r->Get()->GetRaftLog()->NextEnts();
    entsEqual(g, wents);
  }
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
TEST(Raft, FollowerCheckMsgApp) {
  craft::EntryPtrs ents = {
    NEW_ENT().Term(1).Index(1)(),
    NEW_ENT().Term(2).Index(2)(),
  };
  struct Test {
    uint64_t term;
    uint64_t index;
    uint64_t windex;
    bool wreject;
    uint64_t wreject_hint;
    uint64_t wlog_term;
  };
  std::vector<Test> tests = {
		// match with committed entries
    {0, 0, 1, false, 0, 0},
    {ents[0]->term(), ents[0]->index(), 1, false, 0, 0},
    // match with uncommitted entries
    {ents[1]->term(), ents[1]->index(), 2, false, 0, 0},

    // unmatch with existing entry
    {ents[0]->term(), ents[1]->index(), ents[1]->index(), true, 1, 1},
    // unexisting entry
    {ents[1]->term() + 1, ents[1]->index() + 1, ents[1]->index() + 1, true, 2, 2},
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    storage->Append(ents);
    auto r = newTestRaft(1, 10, 1, storage);
    raftpb::HardState hs;
    hs.set_commit(1);
    r->Get()->LoadState(hs);
    r->Get()->BecomeFollower(2, 2);

    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(tt.term).Index(tt.index)());

    auto msgs = r->ReadMessages();
    craft::MsgPtrs wmsgs = {
      NEW_MSG().From(1).To(2).Type(raftpb::MessageType::MsgAppResp).Term(2).Index(tt.windex).Reject(tt.wreject).LogTerm(tt.wlog_term)(),
    };
    msgsEqual(msgs, wmsgs);
  }
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
TEST(Raft, FollowerAppendEntries) {
  struct Test {
    uint64_t index;
    uint64_t term;
    craft::EntryPtrs ents;
    craft::EntryPtrs wents;
    craft::EntryPtrs wunstable;
  };
  std::vector<Test> tests = {
    {
      2, 2,
      {NEW_ENT().Term(3).Index(3)()},
      {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(2).Index(2)(), NEW_ENT().Term(3).Index(3)()},
      {NEW_ENT().Term(3).Index(3)()},
    },
    {
      1, 1,
      {NEW_ENT().Term(3).Index(2)(), NEW_ENT().Term(4).Index(3)()},
      {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(3).Index(2)(), NEW_ENT().Term(4).Index(3)()},
      {NEW_ENT().Term(3).Index(2)(), NEW_ENT().Term(4).Index(3)()},
    },
    {
      0, 0,
      {NEW_ENT().Term(1).Index(1)()},
      {NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(2).Index(2)()},
      {},
    },
    {
      0, 0,
      {NEW_ENT().Term(3).Index(1)()},
      {NEW_ENT().Term(3).Index(1)()},
      {NEW_ENT().Term(3).Index(1)()},
    }
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    storage->Append({NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(2).Index(2)()});
    auto r = newTestRaft(1, 10, 1, storage);
    r->Get()->BecomeFollower(2, 2);

    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgApp).Term(2).LogTerm(tt.term).Index(tt.index).Entries(tt.ents)());
    entsEqual(r->Get()->GetRaftLog()->AllEntries(), tt.wents);
    entsEqual(r->Get()->GetRaftLog()->UnstableEntries(), tt.wunstable);
  }
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
TEST(Raft, LeaderSyncFollowerLog) {
  craft::EntryPtrs ents = {
    NEW_ENT()(),
    NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
    NEW_ENT().Term(4).Index(4)(), NEW_ENT().Term(4).Index(5)(),
    NEW_ENT().Term(5).Index(6)(), NEW_ENT().Term(5).Index(7)(),
    NEW_ENT().Term(6).Index(8)(), NEW_ENT().Term(6).Index(9)(), NEW_ENT().Term(6).Index(10)(),
  };
  uint64_t term = 8;
  std::vector<craft::EntryPtrs> tests = {
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(4).Index(4)(), NEW_ENT().Term(4).Index(5)(),
      NEW_ENT().Term(5).Index(6)(), NEW_ENT().Term(5).Index(7)(),
      NEW_ENT().Term(6).Index(8)(), NEW_ENT().Term(6).Index(9)(),
    },
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(4).Index(4)(),
    },
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(4).Index(4)(), NEW_ENT().Term(4).Index(5)(),
      NEW_ENT().Term(5).Index(6)(), NEW_ENT().Term(5).Index(7)(),
      NEW_ENT().Term(6).Index(8)(), NEW_ENT().Term(6).Index(9)(), NEW_ENT().Term(6).Index(10)(), NEW_ENT().Term(6).Index(11)(),
    },
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(4).Index(4)(), NEW_ENT().Term(4).Index(5)(),
      NEW_ENT().Term(5).Index(6)(), NEW_ENT().Term(5).Index(7)(),
      NEW_ENT().Term(6).Index(8)(), NEW_ENT().Term(6).Index(9)(), NEW_ENT().Term(6).Index(10)(),
      NEW_ENT().Term(7).Index(11)(), NEW_ENT().Term(7).Index(12)(),
    },
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(4).Index(4)(), NEW_ENT().Term(4).Index(5)(), NEW_ENT().Term(4).Index(6)(), NEW_ENT().Term(4).Index(7)(),
    },
    {
      NEW_ENT()(),
      NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)(), NEW_ENT().Term(1).Index(3)(),
      NEW_ENT().Term(2).Index(4)(), NEW_ENT().Term(2).Index(5)(), NEW_ENT().Term(2).Index(6)(),
      NEW_ENT().Term(3).Index(7)(), NEW_ENT().Term(3).Index(8)(), NEW_ENT().Term(3).Index(9)(), NEW_ENT().Term(3).Index(10)(),
    }
  };
  for (auto& tt : tests) {
    auto lead_storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    lead_storage->Append(ents);
    auto lead = newTestRaft(1, 10, 1, lead_storage);
    raftpb::HardState lhs;
    lhs.set_commit(lead->Get()->GetRaftLog()->LastIndex());
    lhs.set_term(term);
    lead->Get()->LoadState(lhs);
    auto follower_storage = newTestMemoryStorage({withPeers({1, 2, 3})});
    follower_storage->Append(tt);
    auto follower = newTestRaft(2, 10, 1, follower_storage);
    raftpb::HardState fhs;
    fhs.set_term(term - 1);
    follower->Get()->LoadState(fhs);
		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
    auto n = NetWork::New({lead, follower, BlackHole::New()});
    n->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
		// The election occurs in the term after the one we loaded with
		// lead.loadState above.
    n->Send({NEW_MSG().From(3).To(1).Type(raftpb::MessageType::MsgVoteResp).Term(term + 1)()});

    n->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
    ASSERT_EQ(raftlogString(lead->Get()->GetRaftLog()), raftlogString(follower->Get()->GetRaftLog()));
  }
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
TEST(Raft, VoteRequest) {
  struct Test {
    craft::EntryPtrs ents;
    uint64_t wterm;
  };
  std::vector<Test> tests = {
    {{NEW_ENT().Term(1).Index(1)()}, 2},
    {{NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(2).Index(2)()}, 3},
  };
  for (auto& tt : tests) {
    auto r = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgApp).Term(tt.wterm - 1).LogTerm(0).Index(0).Entries(tt.ents)());
    r->ReadMessages();

    for (int64_t i = 1; i < r->Get()->ElectionTimeout() * 2; i++) {
      r->Get()->TickElection();
    }

    auto msgs = r->ReadMessages();
    msgsSort(msgs);
    ASSERT_EQ(msgs.size(), static_cast<size_t>(2));
    for (size_t i = 0; i < msgs.size(); i++) {
      auto m = msgs[i];
      ASSERT_EQ(m->type(), raftpb::MessageType::MsgVote);
      ASSERT_EQ(m->to(), i+2);
      ASSERT_EQ(m->term(), tt.wterm);
      auto windex = tt.ents[tt.ents.size() - 1]->index();
      auto wlogterm = tt.ents[tt.ents.size() - 1]->term();
      ASSERT_EQ(m->index(), windex);
      ASSERT_EQ(m->logterm(), wlogterm);
    }
  }
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
TEST(Raft, Voter) {
  struct Test {
    craft::EntryPtrs ents;
    uint64_t logterm;
    uint64_t index;

    bool wreject;
  };
  std::vector<Test> tests = {
    // same logterm
    {{NEW_ENT().Term(1).Index(1)()}, 1, 1, false},
    {{NEW_ENT().Term(1).Index(1)()}, 1, 2, false},
    {{NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)()}, 1, 1, true},
    // candidate higher logterm
    {{NEW_ENT().Term(1).Index(1)()}, 2, 1, false},
    {{NEW_ENT().Term(1).Index(1)()}, 2, 2, false},
    {{NEW_ENT().Term(1).Index(1)(), NEW_ENT().Term(1).Index(2)()}, 2, 1, false},
    // voter higher logterm
    {{NEW_ENT().Term(2).Index(1)()}, 1, 1, true},
    {{NEW_ENT().Term(2).Index(1)()}, 1, 2, true},
    {{NEW_ENT().Term(2).Index(1)(), NEW_ENT().Term(1).Index(2)()}, 1, 1, true},
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2})});
    storage->Append(tt.ents);
    auto r = newTestRaft(1, 10, 1, storage);

    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgVote).Term(3).LogTerm(tt.logterm).Index(tt.index)());

    auto msgs = r->ReadMessages();
    ASSERT_EQ(msgs.size(), static_cast<size_t>(1));
    auto m = msgs[0];
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgVoteResp);
    ASSERT_EQ(m->reject(), tt.wreject);
  }
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
TEST(Raft, LeaderOnlyCommitsLogFromCurrentTerm) {
  craft::EntryPtrs ents = {
    NEW_ENT().Term(1).Index(1)(),
    NEW_ENT().Term(2).Index(2)(),
  };
  struct Test {
    uint64_t index;
    uint64_t wcommit;
  };
  std::vector<Test> tests = {
		// do not commit log entries in previous terms
    {1, 0},
    {2, 0},
    // commit log in current term
    {3, 3},
  };
  for (auto& tt : tests) {
    auto storage = newTestMemoryStorage({withPeers({1, 2})});
    storage->Append(ents);
    auto r = newTestRaft(1, 10, 1, storage);
    raftpb::HardState hs;
    hs.set_term(2);
    r->Get()->LoadState(hs);
    // become leader at term 3
    r->Get()->BecomeCandidate();
    r->Get()->BecomeLeader();
    r->ReadMessages();
    // propose a entry to current term
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})());

    r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Term(r->Get()->Term()).Index(tt.index)());
    ASSERT_EQ(r->Get()->GetRaftLog()->Committed(), tt.wcommit);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}