#include "raft_test_util.h"

#include "util.h"

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
      if (craft::Util::Random(0, 99) < static_cast<int>(perc)) {
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

std::vector<uint64_t> idsBySize(size_t size) {
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < size; i++) {
    ids.push_back(static_cast<uint64_t>(i) + 1);
  }
  return ids;
};

std::shared_ptr<craft::MemoryStorage> newTestMemoryStorage(std::vector<testMemoryStorageOptions> opts) {
  auto ms = std::make_shared<craft::MemoryStorage>();
  for (auto& o : opts) {
    o(ms);
  }
  return ms;
}

testMemoryStorageOptions withPeers(std::vector<uint64_t> peers) {
  return [peers](std::shared_ptr<craft::MemoryStorage> ms) {
    auto [snap, s] = ms->SnapShot();
    assert(s.IsOK());
    snap->mutable_metadata()->mutable_conf_state()->mutable_voters()->Clear();
    for (auto peer : peers) {
      snap->mutable_metadata()->mutable_conf_state()->mutable_voters()->Add(peer);
    }
  };
}

testMemoryStorageOptions withLearners(std::vector<uint64_t> learners) {
  return [learners](std::shared_ptr<craft::MemoryStorage> ms) {
    auto [snap, s] = ms->SnapShot();
    assert(s.IsOK());
    snap->mutable_metadata()->mutable_conf_state()->mutable_learners()->Clear();
    for (auto learner : learners) {
      snap->mutable_metadata()->mutable_conf_state()->mutable_learners()->Add(learner);
    }
  };
}

craft::Raft::Config newTestConfig(uint64_t id, int64_t election, int64_t heartbeat, std::shared_ptr<craft::Storage> storage) {
  return craft::Raft::Config{
    .id = id,
    .election_tick = election,
    .heartbeat_tick = heartbeat,
    .storage = storage,
    .max_size_per_msg = craft::Raft::kNoLimit,
    .max_inflight_msgs = 256,
  };
}

std::shared_ptr<Raft> newTestRaft(uint64_t id, uint64_t election, uint64_t heartbeat, std::shared_ptr<craft::Storage> storage) {
  auto cfg = newTestConfig(id, election, heartbeat, storage);
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

std::shared_ptr<Raft> newTestRaftWithConfig(craft::Raft::Config& cfg) {
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

std::shared_ptr<Raft> newTestLearnerRaft(uint64_t id, uint64_t election, uint64_t hearbeat, std::shared_ptr<craft::Storage> storage) {
  auto cfg = newTestConfig(id, election, hearbeat, storage);
  return std::make_shared<Raft>(craft::Raft::New(cfg));
}

void preVoteConfig(craft::Raft::Config& cfg) {
  cfg.pre_vote = true;
}

std::shared_ptr<Raft> entsWithConfig(NetWork::ConfigFunc config_func, std::vector<uint64_t> terms) {
  auto storage = std::make_shared<craft::MemoryStorage>();
  for (size_t i = 0; i < terms.size(); i++) {
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

std::shared_ptr<Raft> votedWithConfig(NetWork::ConfigFunc config_func, uint64_t vote, uint64_t term) {
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

raftpb::ConfChangeV2 makeConfChange(uint64_t id, raftpb::ConfChangeType type) {
  raftpb::ConfChange cc;
  cc.set_node_id(id);
  cc.set_type(type);
  craft::ConfChangeI cci(std::move(cc));
  return cci.AsV2();
}

raftpb::ConfChangeV2 makeConfChange(std::vector<std::pair<uint64_t, raftpb::ConfChangeType>> ccs) {
  raftpb::ConfChangeV2 cc_v2;
  for (auto& p : ccs) {
    auto change_single = cc_v2.add_changes();
    change_single->set_type(p.second);
    change_single->set_node_id(p.first);
  }
  return cc_v2;
}

std::string raftlogString(craft::RaftLog* l) {
  std::stringstream ss;
  ss << "committed: " << l->Committed() << std::endl;
  ss << "applied: " << l->Applied() << std::endl;
  auto ents = l->AllEntries();
  for (size_t i = 0; i < ents.size(); i++) {
    ss << "#" << i << ": " << ents[i]->SerializeAsString() << std::endl;
  }
  return ss.str();
}