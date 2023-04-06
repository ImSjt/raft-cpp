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

#include <storage_test>

namespace craft {

EntryPtrs Util::LimitSize(EntryPtrs&& ents, uint64_t max_size) {
  if (ents.empty()) {
    return EntryPtrs();
  }

  uint64_t size = 0;
  for (auto it = ents.begin(); it != ents.end(); ++it) {
    size += static_cast<uint64_t>((*it)->ByteSizeLong());
    if (size > max_size) {
      ents.erase(it, ents.end());
      break;
    }
  }
  return std::move(ents);
}

EntryPtrs Util::MakeEntries(MsgPtr msg) {
  EntryPtrs entries;
  for (size_t i = 0; i < msg->entries_size(); i++) {
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

std::vector<std::string> Util::Split(const std::string& s, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream token_stream(s);
  while (std::getline(token_stream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

}  // namespace craft