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

namespace craft {

// ReadState provides state for read only query.
// It's caller's responsibility to call read_index_ first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through request_ctx_, eg. given a unique id as
// request_ctx_
struct ReadState {
  uint64_t index_;
  std::string request_ctx_;
};

struct ReadIndexStatus {
  // Message req_;
  uint64_t index_;
  std::map<uint64_t, bool> acks_;
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

  const ReadOnlyOption& GetReadOnlyOption() const { return option_; }

 private:
  ReadOnlyOption option_;
  std::map<std::string, std::shared_ptr<ReadIndexStatus>> pending_read_index_;
  std::list<std::string> read_index_queue_;
};

}  // namespace craft