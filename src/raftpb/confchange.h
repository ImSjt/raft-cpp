// Copyright 2023 juntaosu
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

#include <string>
#include <optional>
#include <vector>

#include "src/raftpb/raft.pb.h"
#include "src/status.h"
#include "src/logger.h"

namespace craft {

class ConfChangeI {
 public:
  ConfChangeI() = default;
  ConfChangeI(const raftpb::ConfChange& cc) : cc_(cc) {}
  ConfChangeI(const raftpb::ConfChangeV2& cc) : cc_v2_(cc) {}
  ConfChangeI(raftpb::ConfChange&& cc) : cc_(std::move(cc)) {}
  ConfChangeI(raftpb::ConfChangeV2&& cc) : cc_v2_(std::move(cc)) {}

  void SetConfChange(raftpb::ConfChange&& cc) { cc_ = std::move(cc); }
  void SetConfChange(raftpb::ConfChangeV2&& cc) { cc_v2_ = std::move(cc); }

  bool IsNull() { return !cc_.has_value() && !cc_v2_.has_value(); }

  raftpb::ConfChangeV2 AsV2() const;
  std::tuple<raftpb::ConfChange, bool> AsV1() const;

  std::tuple<raftpb::EntryType, std::string, bool> Marshal() const;

 private:
  std::optional<raftpb::ConfChange> cc_;
  std::optional<raftpb::ConfChangeV2> cc_v2_;
};

// EnterJoint returns two bools. The second bool is true if and only if this
// config change will use Joint Consensus, which is the case if it contains more
// than one change or if the use of Joint Consensus was requested explicitly.
// The first bool can only be true if second one is, and indicates whether the
// Joint State will be left automatically.
std::tuple<bool, bool> EnterJoint(std::shared_ptr<Logger> logger, const raftpb::ConfChangeV2& cc);

// LeaveJoint is true if the configuration change leaves a joint configuration.
// This is the case if the ConfChangeV2 is zero, with the possible exception of
// the Context field.
bool LeaveJoint(raftpb::ConfChangeV2& cc);

// ConfChangesFromString parses a Space-delimited sequence of operations into a
// slice of ConfChangeSingle. The supported operations are:
// - vn: make n a voter,
// - ln: make n a learner,
// - rn: remove n, and
// - un: update n.
std::tuple<std::vector<raftpb::ConfChangeSingle>, Status> ConfChangesFromString(
    const std::string& s);

// ConfChangesToString is the inverse to ConfChangesFromString.
std::string ConfChangesToString(const std::vector<raftpb::ConfChangeSingle>& ccs);

}  // namespace craft