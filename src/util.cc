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
#include "util.h"

namespace craft {

EntryPtrs Util::LimitSize(EntryPtrs&& ents, uint64_t max_size) {
  if (ents.empty()) {
    return EntryPtrs();
  }

  uint64_t size = 0;
  for (auto it = ents.begin(); it != ents.end(); ++it) {
    size += static_cast<uint64_t>((*it)->ByteSizeLong());
    if (it == ents.begin()) {
      continue;
    }
    if (size > max_size) {
      ents.erase(it, ents.end());
      break;
    }
  }
  return std::move(ents);
}

EntryPtrs Util::MakeEntries(MsgPtr msg) {
  EntryPtrs entries;
  for (int i = 0; i < msg->entries_size(); i++) {
    entries.emplace_back(
        std::make_shared<raftpb::Entry>(std::move(*(msg->mutable_entries(i)))));
  }
  return entries;
}

MsgPtr Util::MakeMsg(const EntryPtrs& ents) {
  auto m = std::make_shared<raftpb::Message>();
  for (auto& ent : ents) {
    m->add_entries()->CopyFrom(*ent);
  }
  return m;
}

std::vector<std::string_view> Util::Split(std::string_view str, char delimiter) {
  std::vector<std::string_view> substrings;
  size_t start = 0;
  size_t end = str.find(delimiter);
  while (end != std::string::npos) {
    substrings.push_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }
  substrings.push_back(str.substr(start));
  return substrings;
}

std::vector<std::string> Util::Split(const std::string& str, char delimiter) {
  std::vector<std::string> substrings;
  size_t start = 0;
  size_t end = str.find(delimiter);
  while (end != std::string::npos) {
    substrings.push_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }
  substrings.push_back(str.substr(start));
  return substrings;
}

std::string Util::Trim(const std::string& str, char c) {
  auto start = str.find_first_not_of(c);
  auto end = str.find_last_not_of(c);
  if (start == std::string::npos || end == std::string::npos) {
    return "";
  }
  return str.substr(start, end - start + 1);
}

bool Util::IsLocalMsg(raftpb::MessageType msgt) {
  return msgt == raftpb::MessageType::MsgHup ||
         msgt == raftpb::MessageType::MsgBeat ||
         msgt == raftpb::MessageType::MsgUnreachable ||
         msgt == raftpb::MessageType::MsgSnapStatus ||
         msgt == raftpb::MessageType::MsgCheckQuorum;
}

bool Util::IsResponseMsg(raftpb::MessageType msgt) {
  return msgt == raftpb::MessageType::MsgAppResp ||
         msgt == raftpb::MessageType::MsgVoteResp ||
         msgt == raftpb::MessageType::MsgHeartbeatResp ||
         msgt == raftpb::MessageType::MsgUnreachable ||
         msgt == raftpb::MessageType::MsgPreVoteResp;
}

}  // namespace craft