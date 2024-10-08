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
#include "src/raftpb/confchange.h"

#include <sstream>

#include "src/logger.h"
#include "src/util.h"

namespace craft {

raftpb::ConfChangeV2 ConfChangeI::AsV2() const {
  if (!cc_v2_) {
    raftpb::ConfChangeV2 cc_v2;
    auto change_single = cc_v2.add_changes();
    change_single->set_type(cc_->type());
    change_single->set_node_id(cc_->node_id());
    cc_v2.set_context(cc_->context());
    return cc_v2;
  }
  return *cc_v2_;
}

std::tuple<raftpb::ConfChange, bool> ConfChangeI::AsV1() const {
  if (!cc_) {
    return std::make_tuple(raftpb::ConfChange(), false);    
  }
  return std::make_tuple(*cc_, true);
}

std::tuple<raftpb::EntryType, std::string, bool> ConfChangeI::Marshal() const {
  raftpb::EntryType type;
  std::string ccdata;
  bool res;
  auto [ccv1, ok] = AsV1();
  if (ok) {
    type = raftpb::EntryType::EntryConfChange;
    res = ccv1.AppendToString(&ccdata);
  } else {
    auto ccv2 = AsV2();
    type = raftpb::EntryType::EntryConfChangeV2;
    res = ccv2.AppendToString(&ccdata);
  }
  return std::make_tuple(type, std::move(ccdata), res);
}

std::tuple<bool, bool> EnterJoint(std::shared_ptr<Logger> logger, const raftpb::ConfChangeV2& cc) {
	// NB: in theory, more config changes could qualify for the "simple"
	// protocol but it depends on the config on top of which the changes apply.
	// For example, adding two learners is not OK if both nodes are part of the
	// base config (i.e. two voters are turned into learners in the process of
	// applying the conf change). In practice, these distinctions should not
	// matter, so we keep it simple and use Joint Consensus liberally.
  if (cc.transition() != raftpb::ConfChangeTransition::ConfChangeTransitionAuto ||
      cc.changes().size() > 1) {
    bool auto_leave = false;
    // Use Joint Consensus.
    switch (cc.transition()) {
      case raftpb::ConfChangeTransition::ConfChangeTransitionAuto:
        auto_leave = true;
        break;
      case raftpb::ConfChangeTransition::ConfChangeTransitionJointImplicit:
        auto_leave = true;
        break;
      case raftpb::ConfChangeTransition::ConfChangeTransitionJointExplicit:
        break;
      default:
        CRAFT_LOG_FATAL(logger, "unknown transition: %d", static_cast<uint32_t>(cc.transition()));
    }
    return std::make_tuple(auto_leave, true);
  }
  return std::make_tuple(false, false);
}

bool LeaveJoint(raftpb::ConfChangeV2& cc) {
  // NB: c is already a copy.
  static raftpb::ConfChangeV2 empty_cc;
  cc.clear_context();
  return cc.changes().empty() && cc.context().empty() && (cc.transition() == empty_cc.transition());
}

std::tuple<std::vector<raftpb::ConfChangeSingle>, Status> ConfChangesFromString(
    const std::string& s) {
  std::vector<raftpb::ConfChangeSingle> ccs;
  auto tokens = Util::Split(Util::Trim(s, ' '), ' ');
  if (!tokens.empty() && tokens[0] == "") {
    tokens.clear();
  }
  for (auto& token : tokens) {
    if (token.size() < 2) {
      return std::make_tuple(std::vector<raftpb::ConfChangeSingle>(), Status::Error("unknow token %s", token.c_str()));
    }
    raftpb::ConfChangeSingle cc;
    if (token[0] == 'v') {
      cc.set_type(raftpb::ConfChangeType::ConfChangeAddNode);
    } else if (token[0] == 'l') {
      cc.set_type(raftpb::ConfChangeType::ConfChangeAddLearnerNode);
    } else if (token[0] == 'r') {
      cc.set_type(raftpb::ConfChangeType::ConfChangeRemoveNode);
    } else if (token[0] == 'u') {
      cc.set_type(raftpb::ConfChangeType::ConfChangeUpdateNode);
    } else {
      return std::make_tuple(std::vector<raftpb::ConfChangeSingle>(), Status::Error("unknow input: %s", token.c_str()));
    }
    uint64_t id;
    try {
      id = std::strtoull(token.data()+1, nullptr, 10);
    } catch (const std::exception& e) {
      return std::make_tuple(std::vector<raftpb::ConfChangeSingle>(), Status::Error("unknow token %s", e.what()));
    }
    cc.set_node_id(id);
    ccs.emplace_back(cc);
  }
  return std::make_tuple(std::move(ccs), Status::OK());
}

std::string ConfChangesToString(const std::vector<raftpb::ConfChangeSingle>& ccs) {
  std::stringstream ss;
  for (size_t i = 0; i < ccs.size(); i++) {
    if (i > 0) {
      ss << " ";
    }
    if (ccs[i].type() == raftpb::ConfChangeType::ConfChangeAddNode) {
      ss << "v";
    } else if (ccs[i].type() == raftpb::ConfChangeType::ConfChangeAddLearnerNode) {
      ss << "l";
    } else if (ccs[i].type() == raftpb::ConfChangeType::ConfChangeRemoveNode) {
      ss << "r";
    } else if (ccs[i].type() == raftpb::ConfChangeType::ConfChangeUpdateNode) {
      ss << "u";
    } else {
      ss << "unknown";
    }
    ss << ccs[i].node_id();
  }
  return ss.str();
}

}  // namespace craft