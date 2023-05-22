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

#include <cstdint>
#include <vector>
#include <random>

#include "define.h"
#include "logger.h"
#include "raftpb/raft.pb.h"

namespace craft {

class Util {
 public:
  static EntryPtrs LimitSize(EntryPtrs&& ents, uint64_t max_size);

  static raftpb::MessageType VoteRespMsgType(raftpb::MessageType msgt) {
    switch (msgt) {
    case raftpb::MessageType::MsgVote:
      return raftpb::MessageType::MsgVoteResp;
    case raftpb::MessageType::MsgPreVote:
      return raftpb::MessageType::MsgPreVoteResp;
    default:
      LOG_FATAL("unexpect type: %d", raftpb::MessageType_Name(msgt).c_str());
      return msgt;
    }
  }

  static EntryPtrs MakeEntries(MsgPtr msg);

  static MsgPtr MakeMsg(const EntryPtrs& ents);

  static std::vector<std::string_view> Split(std::string_view str, char delimiter);
  static std::vector<std::string> Split(const std::string& str, char delimiter);

  static std::string Trim(const std::string& str, char c);

  static size_t PayloadSize(const EntryPtr& e) { return e->data().size(); }

  static bool IsLocalMsg(raftpb::MessageType msgt);

  static bool IsResponseMsg(raftpb::MessageType msgt);

  // [lower_bound, upper_bound]
  static int Random(int lower_bound, int upper_bound);

 private:
  static thread_local std::mt19937 gen_;

};

}  // namespace craft