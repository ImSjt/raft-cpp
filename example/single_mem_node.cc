// Copyright 2023 juntaosu
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
#include <iostream>
#include <chrono>
#include <thread>
#include <any>
#include <functional>

#include "src/rawnode.h"
#include "blockingconcurrentqueue.h"

struct Peer {
  std::unique_ptr<craft::RawNode> rn;
  std::shared_ptr<craft::MemoryStorage> storage;
};

struct Request {
  int id;
  std::function<void()> cb;
};

static void onReady(Peer& peer, std::map<std::string, std::function<void()>>& waiters) {
  if (!peer.rn->HasReady()) {
    return;
  }
  auto rd = peer.rn->GetReady();

  // Persistent hard state and entries.
  // rd.hard_state / rd.entries

  if (!craft::IsEmptySnap(rd->snapshot)) {
    // Persistent snapshot
    peer.storage->ApplySnapshot(rd->snapshot);
    // Apply snapshot
  }

  peer.storage->Append(rd->entries);

  auto handle_messages = [](const craft::MsgPtrs& msgs) {
  };
  handle_messages(rd->messages);

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
  handle_committed_entries(rd->committed_entries);

  peer.rn->Advance();
}

static void sendPropose(std::shared_ptr<craft::Logger> logger, moodycamel::BlockingConcurrentQueue<std::any>& q) {
  int i = 0;
  while (1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    q.enqueue(Request{.id = ++i, .cb = [i, logger]() {
      CRAFT_LOG_DEBUG(logger, "%d has been applied, responding to the client.", i);
    }});
    q.enqueue(Request{.id = ++i, .cb = [i, logger]() {
      CRAFT_LOG_DEBUG(logger, "%d has been applied, responding to the client.", i);
    }});
  }
}

int main(int argc, char* argv[]) {
  std::map<std::string, std::function<void()>> waiters;

  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto storage = std::make_shared<craft::MemoryStorage>(logger);
  craft::Raft::Config cfg {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
    .logger = logger,
  };
  Peer peer {
    .rn = craft::RawNode::Start(cfg, {craft::Peer{1}}),
    .storage = storage,
  };

  moodycamel::BlockingConcurrentQueue<std::any> q;
  auto thread = std::thread([logger, &q]() {
    sendPropose(logger, q);
  });

  int64_t heartbeat_timeout = 100;
  int64_t wait_timeout = heartbeat_timeout;
  auto hearbeat_time = std::chrono::system_clock::now();
  while (1) {
    std::vector<std::any> items(10);
    size_t count = q.wait_dequeue_bulk_timed(items.begin(), 10, std::chrono::milliseconds(wait_timeout));
    for (size_t i = 0; i < count; i++) {
      auto& item = items[i];
      if (item.type() == typeid(Request)) {
        auto req = std::any_cast<Request>(std::move(item));
        std::string id = std::to_string(req.id);
        waiters[id] = std::move(req.cb);  // register
        std::string data = id;  // encode request
        peer.rn->Propose(data);
      } else if (item.type() == typeid(craft::MsgPtr)) {
        auto m = std::any_cast<craft::MsgPtr>(std::move(item));
        peer.rn->Step(m);
      }
    }

    auto now = std::chrono::system_clock::now();
    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - hearbeat_time).count() > heartbeat_timeout) {
      hearbeat_time = now;
      wait_timeout = heartbeat_timeout;
      peer.rn->Tick();
    } else {
      wait_timeout = heartbeat_timeout - std::chrono::duration_cast<std::chrono::milliseconds>(now - hearbeat_time).count();
    }

    onReady(peer, waiters);
  }

  thread.join();

  return 0;
}