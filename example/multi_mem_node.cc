#include <iostream>
#include <chrono>
#include <thread>
#include <any>
#include <functional>
#include <future>
#include <mutex>

#include "src/rawnode.h"
#include "blockingconcurrentqueue.h"

// <node_id, req_id>
using ReqID = std::pair<int64_t, int64_t>;

class Waiters {
 public:
  std::future<void> Wait(ReqID id) {
    std::lock_guard lg(lock_);
    return waiters_[id].get_future();
  }

  void Trigger(ReqID id) {
    std::lock_guard lg(lock_);
    auto it = waiters_.find(id);
    if (it != waiters_.end()) {
      it->second.set_value();
    }
  }

 private:
  std::mutex lock_;
  std::map<ReqID, std::promise<void>> waiters_;
};

class Store {
 public:
  void Add(int64_t key, int64_t value) {
    std::lock_guard lg(lock_);
    kvs[key] = value;
  }

  int64_t Get(int64_t key) {
    std::lock_guard lg(lock_);
    auto it = kvs.find(key);
    if (it == kvs.end()) {
      return 0;
    }
    return it->second;
  }

 private:
  std::mutex lock_;
  std::map<int64_t, int64_t> kvs;
};

struct Node {
  std::unique_ptr<craft::RawNode> rn;
  std::shared_ptr<craft::MemoryStorage> storage;
  std::shared_ptr<moodycamel::BlockingConcurrentQueue<std::any>> msg_channel;
  Store store;
  Waiters waiters;
};

struct Propose {
  ReqID id;
  int64_t key;
  int64_t value;

  std::string Encode() const {
    std::string data;
    data.assign(reinterpret_cast<const char*>(this), sizeof(Propose));
    return data;
  }
  void Decode(const std::string& data) {
    memcpy(reinterpret_cast<char*>(this), data.c_str(), std::min(data.size(), sizeof(Propose)));
  }
};

class Transport {
 public:
  void Send(craft::MsgPtr msg) {
    std::lock_guard lg(lock_);
    msg_channels_[msg->to()]->enqueue(msg);
  }

  void Add(uint64_t id, std::shared_ptr<moodycamel::BlockingConcurrentQueue<std::any>> msg_channel) {
    std::lock_guard lg(lock_);
    msg_channels_[id] = msg_channel;
  }

 private:
  std::mutex lock_;
  std::map<uint64_t, std::shared_ptr<moodycamel::BlockingConcurrentQueue<std::any>>> msg_channels_;
};

static void onReady(std::shared_ptr<Node> node, Transport& transport) {
  if (!node->rn->HasReady()) {
    return;
  }
  auto rd = node->rn->GetReady();

  // Persistent hard state and entries.
  // rd.hard_state / rd.entries

  if (!craft::IsEmptySnap(rd.snapshot)) {
    // Persistent snapshot
    node->storage->ApplySnapshot(rd.snapshot);
    // Apply snapshot
  }

  node->storage->Append(rd.entries);

  auto handle_messages = [&transport](const craft::MsgPtrs& msgs) {
    for (auto msg : msgs) {
      transport.Send(msg);
    }
  };
  handle_messages(rd.messages);

  auto handle_committed_entries = [node](const craft::EntryPtrs& ents) {
    for (auto& ent : ents) {
      if (ent->data().empty()) {
        // Emtpy entry, when the peer becomes Leader it will send an empty entry.
        continue;
      }

      if (ent->type() == raftpb::EntryType::EntryNormal) {
        Propose propose;
        propose.Decode(ent->data());
        node->store.Add(propose.key, propose.value);
        node->waiters.Trigger(propose.id);
      } else if (ent->type() == raftpb::EntryType::EntryConfChangeV2) {
        raftpb::ConfChangeV2 cc;
        cc.ParseFromString(ent->data());
        node->rn->ApplyConfChange(craft::ConfChangeI(std::move(cc)));
      } else if (ent->type() == raftpb::EntryType::EntryConfChange) {
        raftpb::ConfChange cc;
        cc.ParseFromString(ent->data());
        node->rn->ApplyConfChange(craft::ConfChangeI(std::move(cc)));
      }
    }
  };
  handle_committed_entries(rd.committed_entries);

  node->rn->Advance(rd);
}

static int64_t genId() {
  static std::atomic<int64_t> genid = 1;
  return genid++;
}

static void process(int64_t heartbeat_timeout, std::shared_ptr<Node> node, Transport& transport) {
  int64_t wait_timeout = heartbeat_timeout;
  auto hearbeat_time = std::chrono::system_clock::now();
  while (1) {
    std::vector<std::any> items(10);
    size_t count = node->msg_channel->wait_dequeue_bulk_timed(items.begin(), items.size(), std::chrono::milliseconds(wait_timeout));
    for (size_t i = 0; i < count; i++) {
      auto& item = items[i];
      if (item.type() == typeid(craft::MsgPtr)) {
        auto msg = std::any_cast<craft::MsgPtr>(std::move(item));
        auto s = node->rn->Step(msg);
        if (!s.IsOK()) {
          std::cout << "err " << s.Str() << std::endl;
        }
      } else if (item.type() == typeid(Propose)) {
        auto propose = std::any_cast<Propose>(std::move(item));
        auto data = propose.Encode();
        auto s = node->rn->Propose(data);
        if (!s.IsOK()) {
          std::cout << "err " << s.Str() << std::endl;
          node->waiters.Trigger(propose.id);
        }
      }
    }

    auto now = std::chrono::system_clock::now();
    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - hearbeat_time).count() > heartbeat_timeout) {
      hearbeat_time = now;
      wait_timeout = heartbeat_timeout;
      node->rn->Tick();
    } else {
      wait_timeout = heartbeat_timeout - std::chrono::duration_cast<std::chrono::milliseconds>(now - hearbeat_time).count();
    }

    onReady(node, transport);
  }
}

int main(int argc, char* argv[]) {
  uint64_t node_num = 5;
  auto logger = std::make_shared<craft::ConsoleLogger>();
  std::map<uint64_t, std::shared_ptr<Node>> nodes;
  std::vector<craft::Peer> peers;
  std::vector<std::thread> threads;
  Transport transport;

  for (uint64_t id = 1; id <= node_num; id++) {
    peers.emplace_back(craft::Peer{id});
  }

  int64_t heartbeat_timeout = 100;
  for (uint64_t id = 1; id <= node_num; id++) {
    
    auto storage = std::make_shared<craft::MemoryStorage>(logger);
    craft::Raft::Config cfg {
      .id = id,
      .election_tick = 10,
      .heartbeat_tick = 3,
      .storage = storage,
      .max_size_per_msg = 1024 * 1024 * 1024,
      .max_inflight_msgs = 256,
      .pre_vote = true,
      .logger = logger,
    };
    auto node = std::make_shared<Node>();
    node->rn = craft::RawNode::Start(cfg, peers);
    node->storage = storage;
    node->msg_channel = std::make_shared<moodycamel::BlockingConcurrentQueue<std::any>>();
    nodes[id] = node;
    transport.Add(id, node->msg_channel);

    threads.push_back(std::thread([node, heartbeat_timeout, &transport]() {
      process(heartbeat_timeout, node, transport);
    }));
  };

  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto node = nodes[1];
  // add node
  std::cout << "=================add node=================" << std::endl;
  {
    auto id = node_num + 1;
    auto storage = std::make_shared<craft::MemoryStorage>(logger);
    craft::Raft::Config cfg = {
      .id = id,
      .election_tick = 10,
      .heartbeat_tick = 3,
      .storage = storage,
      .max_size_per_msg = 1024 * 1024 * 1024,
      .max_inflight_msgs = 256,
      .pre_vote = true,
      .logger = logger,
    };
    auto new_node = std::make_shared<Node>();
    new_node->rn = craft::RawNode::ReStart(cfg);
    new_node->storage = storage;
    new_node->msg_channel = std::make_shared<moodycamel::BlockingConcurrentQueue<std::any>>();
    nodes[id] = new_node;
    transport.Add(id, new_node->msg_channel);

    raftpb::ConfChange cc;
    cc.set_node_id(id);
    cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    auto s = node->rn->ProposeConfChange(craft::ConfChangeI(std::move(cc)));
    if (!s.IsOK()) {
      std::cout << "err " << s.Str() << std::endl;
    }

    threads.push_back(std::thread([new_node, heartbeat_timeout, &transport]() {
      process(heartbeat_timeout, new_node, transport);
    }));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  // propose
  {
    while (1) {
      int64_t key = genId();
      int64_t value = genId();
      Propose p = {
        .id = {node->rn->GetRaft()->ID(), genId()},
        .key = key,
        .value = value,
      };
      std::cout << "=================propose=================" << std::endl;
      std::cout << "propose id=" << p.id.first << "_" << p.id.second << " key=" << p.key << " value=" << p.value << std::endl;
      auto fut = node->waiters.Wait(p.id);
      node->msg_channel->enqueue(p);
      fut.wait();
      std::cout << "get from store key=" << p.key << " value=" << node->store.Get(p.key) << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return 0;
}