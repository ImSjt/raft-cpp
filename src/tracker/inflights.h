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
#include <memory>
#include <vector>

namespace craft {

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
class Inflights {
 public:
  Inflights(int32_t size) : start_(0), count_(0), size_(size) {}

  // Clone returns an *Inflights that is identical to but shares no memory with
  // the receiver.
  std::unique_ptr<Inflights> Clone() {
    auto inflights = std::make_unique<Inflights>(*this);
    // return std::move(inflights);
    return inflights;
  }

  // Add notifies the Inflights that a new message with the given index is being
  // dispatched. Full() must be called prior to Add() to verify that there is
  // room for one more message, and consecutive calls to add Add() must provide
  // a monotonic sequence of indexes.
  void Add(uint64_t inflight);

  // FreeLE frees the inflights smaller or equal to the given `to` flight.
  void FreeLE(uint64_t to);

  // FreeFirstOne releases the first inflight. This is a no-op if nothing is
  // inflight.
  void FreeFirstOne();

  // Full returns true if no more messages can be sent at the moment.
  bool Full() { return count_ == size_; }

  // Count returns the number of inflight messages.
  int32_t Count() { return count_; }

  // reset frees all inflights.
  void Reset() {
    count_ = 0;
    start_ = 0;
  }

 private:
  // grow the inflight buffer by doubling up to inflights.size. We grow on
  // demand instead of preallocating to inflights.size to handle systems which
  // have thousands of Raft groups per process.
  void Grow();

 private:
  // the starting index in the buffer
  int64_t start_;
  // number of inflights in the buffer
  int64_t count_;
  // the size of the buffer
  int64_t size_;
  // buffer contains the index of the last entry
  // inside one message.
  std::vector<uint64_t> buffer_;
};

}  // namespace craft