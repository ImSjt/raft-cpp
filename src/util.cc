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
    if (size > max_size) {
      ents.erase(it, ents.end());
      break;
    }
  }
  return std::move(ents);
}

}  // namespace craft