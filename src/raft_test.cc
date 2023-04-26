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
      peers[id] = sm;
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
      raft->Reset(raft->Term());
      peers_[j] = peer;
    } else if (typeid(*peer) == typeid(BlackHole)) {
      peers_[j] = peer;
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

TEST(Raft, ProgressLeader) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomLeader();
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
  r->Get()->BecomLeader();

  r->Get()->GetTracker().GetProgress(2)->SetProbeSent(true);

  r->Step(makeMsg(1, 1, raftpb::MessageType::MsgBeat, {}));
  ASSERT_TRUE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());

  r->Get()->GetTracker().GetProgress(2)->BecomeReplicate();
  r->Step(makeMsg(2, 1, raftpb::MessageType::MsgHeartbeatResp, {}));
  ASSERT_FALSE(r->Get()->GetTracker().GetProgress(2)->ProbeSent());
}

TEST(Raft, ProgressPaused) {
  auto r = newTestRaft(1, 5, 1, newTestMemoryStorage({withPeers({1, 2})}));
  r->Get()->BecomeCandidate();
  r->Get()->BecomLeader();
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
  r->BecomLeader();

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
  auto m = makeMsg(2, 1, raftpb::MessageType::MsgAppResp, {});
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
  auto m2 = makeMsg(2, 1, raftpb::MessageType::MsgAppResp, {});
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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}