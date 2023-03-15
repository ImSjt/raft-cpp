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
#include "tracker/inflights.h"

#include "common/logger.h"

namespace craft {

void Inflights::Add(uint64_t inflight) {
  if (Full()) {
    LOG_FATAL("cannot add into a Full inflights");
  }
  int32_t next = start_ + count_;
  int32_t size = size_;
  if (next >= size) {
    next -= size;
  }
  if (next >= buffer_.size()) {
    Grow();
  }
  buffer_[next] = inflight;
  count_++;
}

void Inflights::FreeLE(uint64_t to) {
  if (count_ == 0 || to < buffer_[start_]) {
    // out of the left side of the window
    return;
  }

  int32_t idx = start_;
  int32_t i;
  for (i = 0; i < count_; i++) {
    if (to < buffer_[idx]) {  // found the first large inflight
      break;
    }

    // increase index and maybe rotate
    idx++;
    if (idx >= size_) {
      idx -= size_;
    }
  }

  // free i inflights and set new start index
  count_ -= i;
  start_ = idx;
  if (count_ == 0) {
    // inflights is empty, reset the start index so that we don't grow the
    // buffer unnecessarily.
    start_ = 0;
  }
}

void Inflights::FreeFirstOne() { FreeLE(buffer_[start_]); }

void Inflights::Grow() {
  int32_t new_size = buffer_.size() * 2;
  if (new_size == 0) {
    new_size = 1;
  } else if (new_size > size_) {
    new_size = size_;
  }
  buffer_.resize(new_size);
}

}  // namespace craft