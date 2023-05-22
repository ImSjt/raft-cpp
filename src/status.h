// Copyright 2023 JT
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

#include <algorithm>
#include <string>

namespace craft {

class Status {
 public:
  Status() : state_(nullptr) {}
  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) : state_(rhs.state_) { rhs.state_ = nullptr; }
  Status& operator=(Status&& rhs);
  // Create a success status.
  static Status OK() { return Status{}; }

  static Status Error(const char* format, ...);

  // Returns true if the status indicates success.
  bool IsOK() const { return (state_ == nullptr); }

  const char* Str() const { return state_ == nullptr ? "OK" : state_; }

 private:
  Status(char* state) : state_(state) {}
  static char* CopyState(char* dst, const char* src);

 private:
  char* state_;
  static const int32_t kStateMaxSize = 1024;
};

inline Status::Status(const Status& rhs) : state_(nullptr) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(state_, rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
  // The following condition catches both aliasing (when this == &rhs),
  // and the common case where both rhs and *this are ok.
  if (state_ != rhs.state_) {
    if (rhs.state_ == nullptr) {
      delete[] state_;
      state_ = nullptr;
    } else {
      state_ = CopyState(state_, rhs.state_);
    }
  }
  return *this;
}

inline Status& Status::operator=(Status&& rhs) {
  std::swap(state_, rhs.state_);
  return *this;
}

}  // namespace craft