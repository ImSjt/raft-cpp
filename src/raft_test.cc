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

class Message {
 public:
  Message() {
    m = std::make_shared<raftpb::Message>();
  }

  Message& Type(raftpb::MessageType type) {
    m->set_type(type);
    return *this;
  }

  Message& From(uint64_t from) {
    m->set_from(from);
    return *this;
  }

  Message& To(uint64_t to) {
    m->set_to(to);
    return *this;
  }

  Message& Term(uint64_t term) {
    m->set_term(term);
    return *this;
  }

  Message& LogTerm(uint64_t log_term) {
    m->set_logterm(log_term);
    return *this;
  }

  Message& Index(uint64_t index) {
    m->set_index(index);
    return *this;
  }

  Message& Entries(craft::EntryPtrs ents) {
    for (auto ent : ents) {
      m->add_entries()->CopyFrom(*ent);
    }
    return *this;
  }

  Message& Commit(uint64_t commit) {
    m->set_commit(commit);
    return *this;
  }

  Message& Snapshot(craft::SnapshotPtr snapshot) {
    m->mutable_snapshot()->CopyFrom(*snapshot);
    return *this;
  }

  Message& Reject(bool reject) {
    m->set_reject(reject);
    return *this;
  }

  Message& RejectHint(uint64_t reject_hint) {
    m->set_rejecthint(reject_hint);
    return *this;
  }

  Message& Context(std::string context) {
    m->set_context(context);
    return *this;
  }

  craft::MsgPtr operator()() {
    return get();
  }

  craft::MsgPtr get() {
    return m;
  }

 private:
  craft::MsgPtr m;
};

class Entry {
 public:
  Entry() {
    ent = std::make_shared<raftpb::Entry>();
  }

  Entry& Term(uint64_t term) {
    ent->set_term(term);
    return *this;
  }

  Entry& Index(uint64_t index) {
    ent->set_index(index);
    return *this;
  }

  Entry& Type(raftpb::EntryType type) {
    ent->set_type(type);
    return *this;
  }

  Entry& Data(std::string data) {
    ent->set_data(data);
    return *this;
  }

  craft::EntryPtr operator()() {
    return get();
  }

  craft::EntryPtr get() {
    return ent;
  }

 private:
  craft::EntryPtr ent;
};

#define NEW_MSG() (Message())
#define NEW_ENT() (Entry())

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

static std::shared_ptr<Raft> entsWithConfig(NetWork::ConfigFunc config_func, std::vector<uint64_t> terms) {
  auto storage = std::make_shared<craft::MemoryStorage>();
  for (size_t i = 0; i < terms.size(); i++) {
    // storage->Append({makeEntry(static_cast<uint64_t>(i + 1), terms[i])});
    storage->Append({NEW_ENT().Index(i+1).Term(terms[i])()});
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
    r->Step(NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data(blob)()})());
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
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(ms[0]->entries(1).index())());
  ms = r->ReadAndClearMsgs();
  ASSERT_EQ(ms.size(), 3);
  for (auto m : ms) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgApp);
    ASSERT_EQ(m->entries().size(), 2);
  }

	// Ack all three of those messages together and get the last two
	// messages (containing three entries).
  r->Step(NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp).Index(ms[2]->entries(1).index())());
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
  auto test_entry = NEW_ENT().Data("testdata")();
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
  ASSERT_EQ(r->UncommittedSize(), 0);

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
  for (size_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
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
  for (size_t i = 0; i < n2->Get()->ElectionTimeout(); i++) {
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
  ASSERT_EQ(sm1->Get()->Term(), 2);

  // Node 1 campaigns again with a higher term. This time it succeeds.
  n->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgHup)()});
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
    auto s = r->Step(NEW_MSG().From(2).To(1).Type(vt).Term(new_term).LogTerm(new_term).Index(42)());
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
  for (size_t i = 0; i < n1->Get()->ElectionTimeout(); i++) {
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
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 3);
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
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  // network recovery
  tt->Recover();
  // avoid committing ChangeTerm proposal
  tt->Ignore(raftpb::MessageType::MsgApp);

  // elect 2 as the new leader with term 2
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});

  // no log entries from previous term should be committed
  sm = std::dynamic_pointer_cast<Raft>(tt->Peers()[2]);
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  tt->Recover();
  // send heartbeat; reset wait
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgBeat)()});
  // append an entry at cuttent term
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT().Data("some data")()})()});
  // expect the committed to be advanced
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 5);
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
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);

  // network recovery
  tt->Recover();

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
  tt->Send({NEW_MSG().From(2).To(2).Type(raftpb::MessageType::MsgHup)()});
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 4);
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
  ASSERT_EQ(a->Get()->Term(), 1);

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
    ASSERT_EQ(sm->Get()->Term(), 1);
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
    ASSERT_EQ(sm->Get()->Term(), 1);
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
    ASSERT_EQ(m.size(), 1);
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
    ASSERT_EQ(m.size(), 1);
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
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);

  // A second heartbeat response generates another MsgApp re-send
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)()});
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);

  // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgAppResp)
            .Index(msgs[0]->index() + msgs[0]->entries().size())()});
  // Consume the message sent in response to MsgAppResp
  sm->ReadMessages();

  // A second heartbeat response generates another MsgApp re-send
  sm->Step({NEW_MSG().From(2).To(1).Type(raftpb::MessageType::MsgHeartbeatResp)()});
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), 0);
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
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgHeartbeat);
  ASSERT_EQ(msgs[0]->context(), ctx);
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetReadIndexQueue().size(), 1);
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetPendingReadIndex().size(), 1);
  ASSERT_TRUE(sm->Get()->GetReadOnly()->GetPendingReadIndex().count(ctx) > 0);

	// heartbeat responses from majority of followers (1 in this case)
	// acknowledge the authority of the leader.
	// more info: raft dissertation 6.4, step 3.
  sm->Step({NEW_MSG().From(2).Type(raftpb::MessageType::MsgHeartbeatResp).Context(ctx)()});
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetReadIndexQueue().size(), 0);
  ASSERT_EQ(sm->Get()->GetReadOnly()->GetPendingReadIndex().size(), 0);
  ASSERT_TRUE(sm->Get()->GetReadOnly()->GetPendingReadIndex().count(ctx) == 0);
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
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 1);
	// Also consume the MsgApp messages that update Commit on the followers.
  sm->ReadMessages();

	// A new command is now proposed on node 1.
  sm->Step(NEW_MSG().From(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})());
	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
  auto msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(msgs[0]->to(), 2);
  ASSERT_EQ(msgs[0]->entries().size(), 1);
  ASSERT_EQ(msgs[0]->entries(0).index(), 2);

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
  sm->Step(NEW_MSG().From(3).Type(raftpb::MessageType::MsgAppResp).Index(1)());
  msgs = sm->ReadMessages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0]->type(), raftpb::MessageType::MsgApp);
  ASSERT_EQ(msgs[0]->to(), 3);
  ASSERT_EQ(msgs[0]->entries().size(), 1);
  ASSERT_EQ(msgs[0]->entries(0).index(), 2);
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
    ASSERT_EQ(msgs.size(), 1);
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

  for (size_t i = 0; i < b->Get()->ElectionTimeout(); i++) {
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
  ASSERT_EQ(b->Get()->Lead(), 1);
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
  ASSERT_EQ(n1->Get()->Term(), 2);
  ASSERT_EQ(n2->Get()->Term(), 2);
  ASSERT_EQ(n3->Get()->Term(), 3);

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
  ASSERT_EQ(n1->Get()->Term(), 3);
  ASSERT_EQ(n2->Get()->Term(), 2);
  ASSERT_EQ(n3->Get()->Term(), 3);
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
    ASSERT_NE(r->Get()->GetReadStates().size(), 0);
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
    ASSERT_NE(r->Get()->GetReadStates().size(), 0);
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
    ASSERT_NE(r->Get()->GetReadStates().size(), 0);
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
  ASSERT_EQ(sm->Get()->GetReadStates().size(), 0);

  nt->Recover();

	// Force peer a to commit a log entry at its term
  for (int64_t i = 0; i < sm->Get()->HeartbeatTimeout(); i++) {
    sm->Get()->Tick();
  }
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgProp).Entries({NEW_ENT()()})()});
  ASSERT_EQ(sm->Get()->GetRaftLog()->Committed(), 4);
  auto last_log_term = sm->Get()->GetRaftLog()->ZeroTermOnErrCompacted(sm->Get()->GetRaftLog()->Term(sm->Get()->GetRaftLog()->Committed()));
  ASSERT_EQ(last_log_term, sm->Get()->Term());

	// Ensure peer a processed postponed read only request after it committed an entry at its term.
  ASSERT_EQ(sm->Get()->GetReadStates().size(), 1);
  auto& rs = sm->Get()->GetReadStates()[0];
  ASSERT_EQ(rs.index, windex);
  ASSERT_EQ(rs.request_ctx, wctx);

	// Ensure peer a accepts read only request after it committed an entry at its term.
  nt->Send({NEW_MSG().From(1).To(1).Type(raftpb::MessageType::MsgReadIndex).Entries({NEW_ENT().Data(wctx)()})()});
  ASSERT_EQ(sm->Get()->GetReadStates().size(), 2);
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
    int wmsg_num;
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
  ASSERT_EQ(msgs.size(), 2);
  std::map<uint64_t, uint64_t> want_commit_map = {
    {2, std::min(sm->Get()->GetRaftLog()->Committed(), sm->Get()->GetTracker().GetProgress(2)->Match())},
    {3, std::min(sm->Get()->GetRaftLog()->Committed(), sm->Get()->GetTracker().GetProgress(3)->Match())},
  };

  for (auto m : msgs) {
    ASSERT_EQ(m->type(), raftpb::MessageType::MsgHeartbeat);
    ASSERT_EQ(m->index(), 0);
    ASSERT_EQ(m->logterm(), 0);
    ASSERT_EQ(want_commit_map[m->to()], m->commit());
    want_commit_map.erase(m->to());
    ASSERT_EQ(m->entries().size(), 0);
  }
}

// tests the output of the state machine when receiving MsgBeat
TEST(Raft, RecvMsgBeat) {
  struct Test {
    craft::RaftStateType state;
    int wmsg;
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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}