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
#include "read_only.h"

#include "logger.h"

namespace craft {

void ReadOnly::AddRequest(uint64_t index, MsgPtr m) {
  auto& s = m->entries(0).data();
  if (pending_read_index_.count(s) != 0) {
    return;
  }
  auto read_index_status = std::make_shared<ReadIndexStatus>();
  read_index_status->req = m;
  read_index_status->index = index;
  pending_read_index_.emplace(std::make_pair(s, read_index_status));
  read_index_queue_.emplace_back(s);
}

const std::map<uint64_t, bool>& ReadOnly::RecvAck(uint64_t id, const std::string& context) {
  auto it = pending_read_index_.find(context);
  if (it == pending_read_index_.end()) {
    static std::map<uint64_t, bool> empty_acks;
    return empty_acks;
  }
  it->second->acks[id] = true;
  return it->second->acks;
}

std::vector<std::shared_ptr<ReadIndexStatus>> ReadOnly::Advance(MsgPtr m) {
  size_t i = 0;
  bool found = false;
  auto& ctx = m->context();
  std::vector<std::shared_ptr<ReadIndexStatus>> rss;
  for (auto& okctx : read_index_queue_) {
    i++;
    auto it = pending_read_index_.find(okctx);
    if (it == pending_read_index_.end()) {
      LOG_FATAL("cannot find corresponding read state from pending map");
    }
    rss.emplace_back(it->second);
    if (okctx == ctx) {
      found = true;
      break;
    }
  }

  if (found) {
    read_index_queue_.erase(read_index_queue_.begin(), read_index_queue_.begin()+i);
    for (auto& rs : rss) {
      pending_read_index_.erase(rs->req->entries(0).data());
    }
    return rss;
  }
  return std::vector<std::shared_ptr<ReadIndexStatus>>();
}

const std::string& ReadOnly::LastPendingRequestCtx() const {
  static std::string empty;
  if (read_index_queue_.empty()) {
    return empty;
  }
  return read_index_queue_[read_index_queue_.size()-1];
}

}  // namespace craft