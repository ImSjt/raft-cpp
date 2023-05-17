#include <iostream>
#include <chrono>
#include <thread>
#include <any>
#include <functional>

#include "rawnode.h"
#include "blockingconcurrentqueue.h"

struct RaftNode {
  std::unique_ptr<craft::RawNode> rn;
  std::shared_ptr<craft::MemoryStorage> storage;
};

struct Request {
  int id;
  std::function<void()> cb;
};

static void onReady(RaftNode& raft_node, std::map<std::string, std::function<void()>>& waiters) {
  if (!raft_node.rn->HasReady()) {
    return;
  }
  auto rd = raft_node.rn->GetReady();

  // Persistent hard state and entries.
  // rd.hard_state / rd.entries

  if (!craft::IsEmptySnap(rd.snapshot)) {
    // Persistent snapshot
    raft_node.storage->ApplySnapshot(rd.snapshot);
    // Apply snapshot
  }

  raft_node.storage->Append(rd.entries);

  auto handle_messages = [](const craft::MsgPtrs& msgs) {
  };
  handle_messages(rd.messages);

  auto handle_committed_entries = [&waiters](const craft::EntryPtrs& ents) {
    for (auto& ent : ents) {
      if (ent->data().empty()) {
        // Emtpy entry, when the peer becomes Leader it will send an empty entry.
        continue;
      }

      if (ent->type() == raftpb::EntryType::EntryNormal) {
        std::string req = ent->data();  // decode request
        std::string id = req;
        waiters[id]();  // trigger
        waiters.erase(id);
      }
    }
  };
  handle_committed_entries(rd.committed_entries);

  raft_node.rn->Advance(rd);
}

static void sendPropose(moodycamel::BlockingConcurrentQueue<std::any>& q) {
  int i = 0;
  while (1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    q.enqueue(Request{.id = ++i, .cb = [i]() {
      std::cout << i << " has been applied, responding to the client." << std::endl;
    }});
    q.enqueue(Request{.id = ++i, .cb = [i]() {
      std::cout << i << " has been applied, responding to the client." << std::endl;
    }});
  }
}

int main(int argc, char* argv[]) {
  std::map<std::string, std::function<void()>> waiters;

  auto storage = std::make_shared<craft::MemoryStorage>();
  craft::Raft::Config cfg {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
  };
  RaftNode raft_node {
    .rn = craft::RawNode::Start(cfg, {craft::Peer{1}}),
    .storage = storage,
  };

  moodycamel::BlockingConcurrentQueue<std::any> q;
  auto thread = std::thread([&q]() {
    sendPropose(q);
  });

  int64_t heartbeat_timeout = 100;
  auto hearbeat_time = std::chrono::system_clock::now();
  while (1) {
    std::vector<std::any> items(10);
    size_t count = q.wait_dequeue_bulk_timed(items.begin(), 10, std::chrono::milliseconds(heartbeat_timeout));
    for (size_t i = 0; i < count; i++) {
      auto& item = items[i];
      if (item.type() == typeid(Request)) {
        auto req = std::any_cast<Request>(std::move(item));
        std::string id = std::to_string(req.id);
        waiters[id] = std::move(req.cb);  // register
        std::string data = id;  // encode request
        raft_node.rn->Propose(data);
      } else if (item.type() == typeid(craft::MsgPtr)) {
        auto m = std::any_cast<craft::MsgPtr>(std::move(item));
        raft_node.rn->Step(m);
      }
    }

    auto now = std::chrono::system_clock::now();
    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - hearbeat_time).count() > heartbeat_timeout) {
      hearbeat_time = now;
      raft_node.rn->Tick();
    }

    onReady(raft_node, waiters);
  }

  thread.join();

  return 0;
}