#include "raft_test_util.h"

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