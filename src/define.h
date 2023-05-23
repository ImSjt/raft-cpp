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

#include <memory>
#include <deque>
#include <vector>
#include <cinttypes>

#include "src/raftpb/raft.pb.h"

namespace craft {

using EntryPtr = std::shared_ptr<raftpb::Entry>;
using EntryPtrs = std::deque<EntryPtr>;

using SnapshotPtr = std::shared_ptr<raftpb::Snapshot>;

using MsgPtr = std::shared_ptr<raftpb::Message>;
using MsgPtrs = std::deque<MsgPtr>;

}  // namespace craft