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
#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <deque>

#include "define.h"

namespace craft {

// ReadState provides state for read only query.
// It's caller's responsibility to call read_index_ first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through request_ctx_, eg. given a unique id as
// request_ctx_
struct ReadState {
  uint64_t index;
  std::string request_ctx;

  bool operator==(const ReadState& other) const {
    return index == other.index && request_ctx == other.request_ctx;
  }
};

struct ReadIndexStatus {
  MsgPtr req;
  uint64_t index;
  std::map<uint64_t, bool> acks;
};

class ReadOnly {
 public:
  enum ReadOnlyOption {
    // kSafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    kSafe,
    // kLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than
    // it
    // should (clock can move backward/pause without any bound). read_index_ is
    // not safe
    // in that case.
    kLeaseBased,
    kNumReadOnlyOption
  };

  ReadOnly(const ReadOnlyOption& option) : option_(option) {}

  // AddRequest adds a read only request into readonly struct.
  // `index` is the commit index of the raft state machine when it received
  // the read only request.
  // `m` is the original read only request message from the local or remote node.
  void AddRequest(uint64_t index, MsgPtr m);

  // RecvAck notifies the readonly struct that the raft state machine received
  // an acknowledgment of the heartbeat that attached with the read only request
  // context.
  const std::map<uint64_t, bool>& RecvAck(uint64_t id, const std::string& context);

  // Advance advances the read only request queue kept by the readonly struct.
  // It dequeues the requests until it finds the read only request that has
  // the same context as the given `m`.
  std::vector<std::shared_ptr<ReadIndexStatus>> Advance(MsgPtr m);

  // LastPendingRequestCtx returns the context of the last pending read only
  // request in readonly struct.
  const std::string& LastPendingRequestCtx() const;

  ReadOnlyOption Option() const { return option_; }
  void SetOption(ReadOnlyOption option) { option_ = option; }

  auto& GetPendingReadIndex() { return pending_read_index_; }
  auto& GetReadIndexQueue() { return read_index_queue_; }

 private:
  ReadOnlyOption option_;
  std::map<std::string, std::shared_ptr<ReadIndexStatus>> pending_read_index_;
  std::deque<std::string> read_index_queue_;
};

}  // namespace craft