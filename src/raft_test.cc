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
#include "util.h"
#include "raftpb/confchange.h"

static std::random_device rd;
static std::mt19937 gen(rd());
 
static int random(int low, int high) {
  std::uniform_int_distribution<> dist(low, high);
  return dist(gen);
}

class StateMachince {
 public:
  virtual craft::Status Step(craft::MsgPtr m) = 0;
  virtual craft::MsgPtrs ReadMessages() = 0;
};

class Raft : public StateMachince {
 public:
  static std::shared_ptr<Raft> New(std::unique_ptr<craft::Raft>&& raft) {
    return std::make_shared<Raft>(std::move(raft));
  }

  Raft(std::unique_ptr<craft::Raft>&& raft) : raft_(std::move(raft)) {}

  craft::Status Step(craft::MsgPtr m) override {
    return raft_->Step(m);
  }

  craft::MsgPtrs ReadMessages() override {
    auto msgs = raft_->Msgs();
    raft_->ClearMsgs();
    return msgs;
  }

  craft::Raft* Get() { return raft_.get(); }

 private:
  std::unique_ptr<craft::Raft> raft_;
};

class BlackHole : public StateMachince {
 public:
  static std::shared_ptr<BlackHole> New() {
    return std::make_shared<BlackHole>();
  }

  craft::Status Step(craft::MsgPtr m) override {
    return craft::Status::OK();
  }

  craft::MsgPtrs ReadMessages() override {
    return {};
  }
};

static std::vector<uint64_t> idsBySize(size_t size) {
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < size; i++) {
    ids.push_back(static_cast<uint64_t>(i) + 1);
  }
  return ids;
};

class NetWork {
 public:
  using Connem = std::pair<uint64_t, uint64_t>;
  using MsgHook = std::function<bool(craft::MsgPtr)>;
  using ConfigFunc = std::function<void(craft::Raft::Config&)>;

  static std::shared_ptr<NetWork> New(std::vector<std::shared_ptr<StateMachince>> peers) {
    return NewWithConfig(nullptr, std::move(peers));
  }

  static std::shared_ptr<NetWork> NewWithConfig(ConfigFunc cfg, std::vector<std::shared_ptr<StateMachince>> peers) {
    auto network = std::make_shared<NetWork>(std::move(cfg), std::move(peers));
    return network;
  }

  NetWork(ConfigFunc cfg, std::vector<std::shared_ptr<StateMachince>> peers);

  void Send(craft::MsgPtrs msgs);

  void Drop(uint64_t from, uint64_t to, uint32_t perc);

  void Cut(uint64_t one, uint64_t other);

  void Isolate(uint64_t id);

  void Ignore(raftpb::MessageType t);

  void Recover();

  craft::MsgPtrs Filter(craft::MsgPtrs msgs);

  std::map<uint64_t, std::shared_ptr<StateMachince>>& Peers() { return peers_; }
  std::map<uint64_t, std::shared_ptr<craft::MemoryStorage>> Storages() { return storages_; }

 private:
  std::map<uint64_t, std::shared_ptr<StateMachince>> peers_;
  std::map<uint64_t, std::shared_ptr<craft::MemoryStorage>> storages_;
  std::map<Connem, uint32_t> dropm_;
  std::set<raftpb::MessageType> ignorem_;

	// msg_hook_ is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
  MsgHook msg_hook_;
};

using testMemoryStorageOptions = std::function<void(std::shared_ptr<craft::MemoryStorage>)>;

static std::shared_ptr<craft::MemoryStorage> newTestMemoryStorage(std::vector<testMemoryStorageOptions> opts) {
  auto ms = std::make_shared<craft::MemoryStorage>();
  for (auto& o : opts) {
    o(ms);
  }
  return ms;
}

static testMemoryStorageOptions withPeers(std::vector<uint64_t> peers) {
  return [peers](std::shared_ptr<craft::MemoryStorage> ms) {
    auto [snap, s] = ms->SnapShot();
    assert(s.IsOK());
    snap->mutable_metadata()->mutable_conf_state()->mutable_voters()->Clear();
    for (auto peer : peers) {
      snap->mutable_metadata()->mutable_conf_state()->mutable_voters()->Add(peer);
    }
  };
}

static testMemoryStorageOptions withLearners(std::vector<uint64_t> learners) {
  return [learners](std::shared_ptr<craft::MemoryStorage> ms) {
    auto [snap, s] = ms->SnapShot();
    assert(s.IsOK());
    snap->mutable_metadata()->mutable_conf_state()->mutable_learners()->Clear();
    for (auto learner : learners) {
      snap->mutable_metadata()->mutable_conf_state()->mutable_learners()->Add(learner);
    }
  };
}

static craft::Raft::Config newTestConfig(uint64_t id, int64_t election, int64_t heartbeat, std::shared_ptr<craft::Storage> storage) {
  return craft::Raft::Config{
    .id = id,
    .election_tick = election,
    .heartbeat_tick = heartbeat,
    .storage = storage,
    .max_size_per_msg = craft::Raft::kNoLimit,
    .max_inflight_msgs = 256,
  };
}

static std::shared_ptr<Raft> newTestRaft(uint64_t id, uint64_t election, uint64_t heartbeat, std::shared_ptr<craft::Storage> storage) {
  auto cfg = newTestConfig(id, election, heartbeat, storage);
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

static std::shared_ptr<Raft> newTestRaftWithConfig(craft::Raft::Config& cfg) {
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

static std::shared_ptr<Raft> newTestLearnerRaft(uint64_t id, uint64_t election, uint64_t hearbeat, std::shared_ptr<craft::Storage> storage) {
  auto cfg = newTestConfig(id, election, hearbeat, storage);
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

static void preVoteConfig(craft::Raft::Config& cfg) {
  cfg.pre_vote = true;
}

NetWork::NetWork(NetWork::ConfigFunc cfg_func, std::vector<std::shared_ptr<StateMachince>> peers) {
  auto size = peers.size();
  auto peer_addrs = idsBySize(size);
  for (size_t j = 0; j < peers.size(); j++) {
    auto id = peer_addrs[j];
    auto& peer = peers[j];
    if (!peer) {
      storages_[id] = newTestMemoryStorage({withPeers(peer_addrs)});
      auto cfg = newTestConfig(id, 10, 1, storages_[id]);
      if (cfg_func) {
        cfg_func(cfg);
      }
      auto sm = std::make_shared<Raft>(craft::Raft::New(cfg));
      peers_[id] = sm;
    } else if (typeid(*peer) == typeid(Raft)) {
      std::set<uint64_t> learners;
      auto sm = std::dynamic_pointer_cast<Raft>(peer);
      auto raft = sm->Get();
      for (auto i : raft->GetTracker().GetConfig().learners_) {
        learners.insert(i);
      }
      raft->SetID(id);
      raft->GetTracker() = craft::ProgressTracker(raft->GetTracker().MaxInflight());

      craft::ProgressMap prs;
      for (size_t i = 0; i < size; i++) {
        auto pr = std::make_shared<craft::Progress>();
        if (learners.count(peer_addrs[i]) > 0) {
          pr->SetIsLearner(true);
          raft->GetTracker().GetConfig().learners_.insert(peer_addrs[i]);
        } else {
          raft->GetTracker().GetConfig().voters_.Incoming().Add(peer_addrs[i]);
        }
        prs[peer_addrs[i]] = pr;
      }
      raft->GetTracker().SetProgressMap(std::move(prs));
      raft->Reset(raft->Term());
      peers_[id] = peer;
    } else if (typeid(*peer) == typeid(BlackHole)) {
      peers_[id] = peer;
    } else {
      std::cout << "unexpected state machine type: " << typeid(*peer).name() << std::endl;
      assert(false);
    }
  }
}

void NetWork::Send(craft::MsgPtrs msgs) {
  while (!msgs.empty()) {
    auto m = msgs[0];
    auto p = peers_[m->to()];
    p->Step(m);
    msgs.pop_front();
    auto nmsgs = Filter(p->ReadMessages());
    msgs.insert(msgs.end(), nmsgs.begin(), nmsgs.end());
  }
}

void NetWork::Drop(uint64_t from, uint64_t to, uint32_t perc) {
  dropm_[{from, to}] = perc;
}

void NetWork::Cut(uint64_t one, uint64_t other) {
  Drop(one, other, 100);  // always drop
  Drop(other, one, 100);  // always drop
}

void NetWork::Isolate(uint64_t id) {
  for (size_t i = 0; i < peers_.size(); i++) {
    auto nid = static_cast<uint64_t>(i) +  1;
    if (nid != id) {
      Drop(id, nid, 100);  // always drop
      Drop(nid, id, 100);  // always drop
    }
  }
}

void NetWork::Ignore(raftpb::MessageType t) {
  ignorem_.insert(t);
}

void NetWork::Recover() {
  dropm_.clear();
  ignorem_.clear();
}

craft::MsgPtrs NetWork::Filter(craft::MsgPtrs msgs) {
  craft::MsgPtrs mm;
  for (auto m : msgs) {
    if (ignorem_.count(m->type()) > 0) {
      continue;
    }
    if (m->type() == raftpb::MsgHup) {
      std::cout << "hups never go over the network, so don't drop them but panic" << std::endl;
      assert(false);
    } else {
      uint32_t perc = 0;
      auto it = dropm_.find({m->from(), m->to()});
      if (it != dropm_.end()) {
        perc = it->second;
      }
      if (random(0, 99) < perc) {
        continue;          
      }
    }
    if (msg_hook_) {
      if (!msg_hook_(m)) {
        continue;
      }
    }
    mm.emplace_back(m);
  }
  return mm;
}

static std::shared_ptr<raftpb::Message> makeMsg(uint64_t from, uint64_t to, raftpb::MessageType type) {
  auto msg = std::make_shared<raftpb::Message>();
  msg->set_from(from);
  msg->set_to(to);
  msg->set_type(type);
  return msg;
}

static std::shared_ptr<raftpb::Message> makeMsg(uint64_t from, uint64_t to, raftpb::MessageType type, std::vector<std::string> ents) {
  auto msg = std::make_shared<raftpb::Message>();
  msg->set_from(from);
  msg->set_to(to);
  msg->set_type(type);
  for (auto& ent : ents) {
    auto e = msg->add_entries();
    e->set_data(ent);
  }
  return msg;
}

static std::shared_ptr<raftpb::Message> makeMsg(uint64_t from, uint64_t to, raftpb::MessageType type, craft::EntryPtrs ents) {
  auto msg = std::make_shared<raftpb::Message>();
  msg->set_from(from);
  msg->set_to(to);
  msg->set_type(type);
  for (auto& ent : ents) {
    auto e = msg->add_entries();
    e->CopyFrom(*ent);
  }
  return msg;
}

static std::shared_ptr<raftpb::Entry> makeEntry(uint64_t index, uint64_t term) {
  auto entry = std::make_shared<raftpb::Entry>();
  entry->set_index(index);
  entry->set_term(term);
  return entry;
}

static std::shared_ptr<raftpb::Entry> makeEntry(uint64_t index, uint64_t term, std::string data) {
  auto entry = std::make_shared<raftpb::Entry>();
  entry->set_index(index);
  entry->set_term(term);
  entry->set_data(data);
  return entry;
}

static std::shared_ptr<Raft> entsWithConfig(NetWork::ConfigFunc config_func, std::vector<uint64_t> terms) {
  auto storage = std::make_shared<craft::MemoryStorage>();
  for (size_t i = 0; i < terms.size(); i++) {
    storage->Append({makeEntry(static_cast<uint64_t>(i + 1), terms[i])});
  }
  auto cfg = newTestConfig(1, 5, 1, storage);
  if (config_func) {
    config_func(cfg);
  }
  auto raft = craft::Raft::New(cfg);
  raft->Reset(terms[terms.size()-1]);
  return Raft::New(std::move(raft));
}

static std::shared_ptr<Raft> votedWithConfig(NetWork::ConfigFunc config_func, uint64_t vote, uint64_t term) {
  auto storage = std::make_shared<craft::MemoryStorage>();
  raftpb::HardState hard_state;
  hard_state.set_vote(vote);
  hard_state.set_term(term);
  storage->SetHardState(hard_state);
  auto cfg = newTestConfig(1, 5, 1, storage);
  if (config_func) {
    config_func(cfg);
  }
  auto raft = craft::Raft::New(cfg);
  raft->Reset(term);
  return Raft::New(std::move(raft));
}

static raftpb::ConfChangeV2 makeConfChange(uint64_t id, raftpb::ConfChangeType type) {
  raftpb::ConfChange cc;
  cc.set_node_id(id);
  cc.set_type(type);
  craft::ConfChangeI cci(std::move(cc));
  return cci.AsV2();
}

static std::string raftlogString(craft::RaftLog* l) {
  std::stringstream ss;
  ss << "committed: " << l->Committed() << std::endl;
  ss << "applied: " << l->Applied() << std::endl;
  auto ents = l->AllEntries();
  for (size_t i = 0; i < ents.size(); i++) {
    ss << "#" << i << ": " << ents[i]->SerializeAsString() << std::endl;
  }
  return ss.str();
}

TEST(Raft, ProgressLeader) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();

  // Send proposals to r1. The first 5 entries should be appended to the log.
  auto prop_msg = makeMsg(1, 1, raftpb::MessageType::MsgProp, {"foo"});
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

  r->Step(makeMsg(1, 1, raftpb::MessageType::MsgBeat));
  ASSERT_TRUE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());

  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();
  r->Step(makeMsg(2, 1, raftpb::MessageType::MsgHeartbeatResp));
  ASSERT_FALSE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());
}

TEST(Raft, ProgressPaused) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomeLeader();
  r->Step(makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"}));
  r->Step(makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"}));
  r->Step(makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"}));

  auto ms = r->ReadMessages();
  ASSERT_EQ(ms.size(), 1);
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
    r->Step(makeMsg(1, 1, raftpb::MessageType::MsgProp, {blob}));
  }

  auto ms = r->ReadAndClearMsgs();
	// First append has two entries: the empty entry to confirm the
	// election, and the first proposal (only one proposal gets sent
	// because we're in probe state).
  ASSERT_EQ(ms.size(), 1);
  ASSERT_EQ(ms[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(ms[0]->entries().size(), 2);
  ASSERT_EQ(ms[0]->entries(0).data().size(), 0);
  ASSERT_EQ(ms[0]->entries(1).data().size(), 1000);

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
  auto m = makeMsg(2, 1, raftpb::MessageType::MsgAppResp);
  m->set_index(ms[0]->entries(1).index());
  r->Step(m);
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), 3);
  for (auto m : ms) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
    ASSERT_EQ(m->entries().size(), 2);
  }

	// Ack all three of those messages together and get the last two
	// messages (containing three entries).
  auto m2 = makeMsg(2, 1, raftpb::MessageType::MsgAppResp);
  m2->set_index(ms[2]->entries(1).index());
  r->Step(m2);
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), 2);
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
  auto test_entry = std::make_shared<raftpb::Entry>();
  test_entry->set_data("testdata");
  size_t max_entry_size = max_entries * craft::Util::PayloadSize(test_entry);

  ASSERT_EQ(craft::Util::PayloadSize(std::make_shared<raftpb::Entry>()), 0);

  auto cfg = newTestConfig(1, 4, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  cfg.max_uncommitted_entries_size = static_cast<uint64_t>(max_entry_size);
  cfg.max_inflight_msgs = 2 * 1024;  // avoid interference
  auto r = craft::Raft::New(cfg);
  r->BecomeCandidate();
  r->BecomeLeader();
  ASSERT_EQ(r->UncommittedSize(), 0);

  // Set the two followers to the replicate state. Commit to tail of log.
  size_t num_followers = 2;
  r->GetTracker().GetProgress(2)->BecomeReplicate();
  r->GetTracker().GetProgress(3)->BecomeReplicate(); 
  r->SetUncommittedSize(0);

  // Send proposals to r1. The first 5 entries should be appended to the log.
  craft::EntryPtrs prop_ents;
  for (size_t i = 0; i < max_entries; i++) {
    auto prop_msg = makeMsg(1, 1, raftpb::MessageType::MsgProp, {test_entry});
    auto s = r->Step(prop_msg);
    ASSERT_TRUE(s.IsOK());
    prop_ents.push_back(test_entry);
  }

  std::cout << r->UncommittedSize() << std::endl;

  // Send one more proposal to r1. It should be rejected.
  auto prop_msg = makeMsg(1, 1, raftpb::MessageType::MsgProp, {test_entry});
  auto s = r->Step(prop_msg);
  ASSERT_FALSE(s.IsOK());
  ASSERT_STREQ(s.Str(), craft::kErrProposalDropped);

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
  auto ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), max_entries * num_followers);
  r->ReduceUncommittedSize(prop_ents);
  ASSERT_EQ(r->UncommittedSize(), 0);

	// Send a single large proposal to r1. Should be accepted even though it
	// pushes us above the limit because we were beneath it before the proposal.
  prop_ents.clear();
  for (size_t i = 0; i < max_entry_size * 2; i++) {
    prop_ents.push_back(test_entry);
  }
  auto prop_msg_large = makeMsg(1, 1, raftpb::MessageType::MsgProp, prop_ents);
  s = r->Step(prop_msg_large);
  ASSERT_TRUE(s.IsOK());

  // Send one more proposal to r1. It should be rejected, again.
  prop_msg = makeMsg(1, 1, raftpb::MessageType::MsgProp, {test_entry});
  s = r->Step(prop_msg);
  ASSERT_FALSE(s.IsOK());
  ASSERT_STREQ(s.Str(), craft::kErrProposalDropped);

	// But we can always append an entry with no Data. This is used both for the
	// leader's first empty entry and for auto-transitioning out of joint config
	// states.
  s = r->Step(makeMsg(1, 1, raftpb::MessageType::MsgProp, {""}));
  ASSERT_TRUE(s.IsOK());

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), num_followers * 2);
  r->ReduceUncommittedSize(prop_ents);
  ASSERT_EQ(r->UncommittedSize(), 0);
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
    tt.network->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
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
  for (size_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(n2->Get()->State(), craft::RaftStateType::kFollower);

  nt->Send({makeMsg(1, 1, raftpb::MessageType::MsgBeat)});

  n1->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  n2->Get()->ApplyConfChange(makeConfChange(2, raftpb::ConfChangeType::ConfChangeAddNode));
  ASSERT_FALSE(n2->Get()->IsLearner());

  // n2 start election, should become leader
  n2->Get()->SetRandomizedElectionTimeout(n2->Get()->ElectionTimeout());
  for (size_t i = 0; i < n2->Get()->ElectionTimeout(); i++) {
    n2->Get()->Tick();
  }

  nt->Send({makeMsg(2, 2, raftpb::MessageType::MsgBeat)});
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

  ASSERT_EQ(n2->Get()->Msgs().size(), 1);
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
    n->Send({makeMsg(campaigner_id, campaigner_id, raftpb::MessageType::MsgHup)});

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
  n->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  auto sm1 = std::dynamic_pointer_cast<Raft>(n->Peers()[1]);
  ASSERT_EQ(sm1->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(sm1->Get()->Term(), 2);

  // Node 1 campaigns again with a higher term. This time it succeeds.
  n->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  ASSERT_EQ(sm1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_EQ(sm1->Get()->Term(), 3);

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
  for (auto& p : n->Peers()) {
    auto sm = std::dynamic_pointer_cast<Raft>(p.second);
    auto entries = sm->Get()->GetRaftLog()->AllEntries();
    ASSERT_EQ(entries.size(), 2);
    ASSERT_EQ(entries[0]->term(), 1);
    ASSERT_EQ(entries[1]->term(), 3);
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
    auto msg = std::make_shared<raftpb::Message>();
    msg->set_from(2);
    msg->set_to(1);
    msg->set_type(vt);
    msg->set_term(new_term);
    msg->set_logterm(new_term);
    msg->set_index(42);
    auto s = r->Step(msg);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(r->Get()->Msgs().size(), 1);
    auto resp = r->Get()->Msgs()[0];
    ASSERT_EQ(resp->type(), craft::Util::VoteRespMsgType(vt));
    ASSERT_FALSE(resp->reject());

		// If this was a real vote, we reset our state and term.
    if (vt == raftpb::MessageType::MsgVote) {
      ASSERT_EQ(r->Get()->State(), craft::RaftStateType::kFollower);
      ASSERT_EQ(r->Get()->Term(), new_term);
      ASSERT_EQ(r->Get()->Vote(), 2);
    } else {
      // In a prevote, nothing changes.
      ASSERT_EQ(r->Get()->State(), st);
      ASSERT_EQ(r->Get()->Term(), orig_term);
			// if st == StateFollower or StatePreCandidate, r hasn't voted yet.
			// In StateCandidate or StateLeader, it's voted for itself.
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
      {makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"})},
      2,
    },
    {
      NetWork::New({nullptr, nullptr, nullptr}),
      {
        makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"}),
        makeMsg(1, 2, raftpb::MessageType::MsgHup),
        makeMsg(1, 2, raftpb::MessageType::MsgProp, {"somedata"}),
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

    tt.network->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
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
  for (size_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
    n1->Get()->Tick();
  }

  nt->Send({makeMsg(1, 1, raftpb::MessageType::MsgBeat)});

	// n1 is leader and n2 is learner
  ASSERT_EQ(n1->Get()->State(), craft::RaftStateType::kLeader);
  ASSERT_TRUE(n2->Get()->IsLearner());

  auto next_committed = n1->Get()->GetRaftLog()->Committed() + 1;
  nt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"})});
  ASSERT_EQ(n1->Get()->GetRaftLog()->Committed(), next_committed);
  ASSERT_EQ(n1->Get()->GetRaftLog()->Committed(), n2->Get()->GetRaftLog()->Committed());

  auto match = n1->Get()->GetTracker().GetProgress(2)->Match();
  ASSERT_EQ(match, n2->Get()->GetRaftLog()->Committed());
}

TEST(Raft, SingleNodeCommit) {
  auto tt = NetWork::New({nullptr});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 3);
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
TEST(Raft, CannotCommitWithoutNewTermEntry) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr, nullptr, nullptr});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});

  // 1 cannot reach 3,4,5
  tt->Cut(1, 3);
  tt->Cut(1, 4);
  tt->Cut(1, 5);

  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  // network recovery
  tt->Recover();
  // avoid committing ChangeTerm proposal
  tt->Ignore(raftpb::MessageType::MsgApp);

  // elect 2 as the new leader with term 2
  tt->Send({makeMsg(2, 2, raftpb::MessageType::MsgHup)});

  // no log entries from previous term should be committed
  sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[2]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  tt->Recover();
  // send heartbeat; reset wait
  tt->Send({makeMsg(2, 2, raftpb::MessageType::MsgBeat)});
  // append an entry at cuttent term
  tt->Send({makeMsg(2, 2, raftpb::MessageType::MsgProp, {"some data"})});
  // expect the committed to be advanced
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 5);
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
TEST(Raft, CommitWithoutNewTermEntry) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr, nullptr, nullptr});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});

  // 1 cannot reach 3,4,5
  tt->Cut(1, 3);
  tt->Cut(1, 4);
  tt->Cut(1, 5);

  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"some data"})});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  // network recovery
  tt->Recover();

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
  tt->Send({makeMsg(2, 2, raftpb::MessageType::MsgHup)});
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 4);
}

TEST(Raft, DuelingCandidates) {
  auto a = newTestRaft(1, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto b = newTestRaft(2, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));
  auto c = newTestRaft(3, 10, 1, newTestMemoryStorage({withPeers({1, 2, 3})}));

  auto nt = NetWork::New({a, b, c});
  nt->Cut(1, 3);

  nt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  nt->Send({makeMsg(3, 3, raftpb::MessageType::MsgHup)});

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
  nt->Send({makeMsg(3, 3, raftpb::MessageType::MsgHup)});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({makeEntry(0, 0), makeEntry(1, 1)});
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

  nt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  nt->Send({makeMsg(3, 3, raftpb::MessageType::MsgHup)});

  // 1 becomes leader since it receives votes from 1 and 2
  auto sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);

  // 3 campaigns then reverts to follower when its PreVote is rejected
  sm = std::dynamic_pointer_cast<Raft>(nt->Peers()[3]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kFollower);

  nt->Recover();

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
  nt->Send({makeMsg(3, 3, raftpb::MessageType::MsgHup)});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({makeEntry(0, 0), makeEntry(1, 1)});
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

  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  tt->Send({makeMsg(3, 3, raftpb::MessageType::MsgHup)});

  // heal the partition
  tt->Recover();
  // send heartbeat; reset wait
  tt->Send({makeMsg(3, 3, raftpb::MessageType::MsgBeat)});

  std::string data = "force follower";
  // send a proposal to 3 to flush out a MsgApp to 1
  tt->Send({makeMsg(3, 3, raftpb::MessageType::MsgProp, {data})});
  // send heartbeat; flush out commit
  tt->Send({makeMsg(3, 3, raftpb::MessageType::MsgBeat)});

  auto a = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(a->Get()->State(), craft::RaftStateType::kFollower);
  ASSERT_EQ(a->Get()->Term(), 1);

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({makeEntry(0, 0), makeEntry(1, 1), makeEntry(2, 1, data)});
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
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, SingleNodePreCandidate) {
  auto tt = NetWork::NewWithConfig(preVoteConfig, {nullptr});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});

  auto sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[1]);
  ASSERT_EQ(sm->Get()->State(), craft::RaftStateType::kLeader);
}

TEST(Raft, OldMessages) {
  auto tt = NetWork::New({nullptr, nullptr, nullptr});
  // make 0 leader @ term 3
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
  tt->Send({makeMsg(2, 2, raftpb::MessageType::MsgHup)});
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgHup)});
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
  auto m = makeMsg(2, 1, raftpb::MessageType::MsgApp);
  m->set_term(2);
  auto e = m->add_entries();
  e->set_index(3);
  e->set_term(2);
  tt->Send({m});
  // commit a new entry
  tt->Send({makeMsg(1, 1, raftpb::MessageType::MsgProp, {"somedata"})});

  auto storage = std::make_shared<craft::MemoryStorage>();
  storage->SetEntries({makeEntry(0, 0), makeEntry(1, 1), makeEntry(2, 2),
                       makeEntry(3, 3), makeEntry(4, 3, "somedata")});
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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}