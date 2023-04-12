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
#include "raftpb/confstate.h"

namespace craft {

bool operator==(const raftpb::ConfState& cs1, const raftpb::ConfState& cs2) {
  auto c_cs1 = cs1;
  auto c_cs2 = cs2;
  for (auto cs : std::vector<raftpb::ConfState*>{&c_cs1, &c_cs2}) {
    std::sort(cs->mutable_voters()->begin(), cs->mutable_voters()->end());
    std::sort(cs->mutable_learners()->begin(), cs->mutable_learners()->end());
    std::sort(cs->mutable_voters_outgoing()->begin(),
              cs->mutable_voters_outgoing()->end());
    std::sort(cs->mutable_learners_next()->begin(),
              cs->mutable_learners_next()->end());
  }

  auto equal =
      [](const ::google::protobuf::RepeatedField< ::google::protobuf::uint64>& v1,
         const ::google::protobuf::RepeatedField< ::google::protobuf::uint64>& v2) {
      return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
    };

  return equal(c_cs1.voters(), c_cs2.voters()) &&
         equal(c_cs1.learners(), c_cs2.learners()) &&
         equal(c_cs1.voters_outgoing(), c_cs2.voters_outgoing()) &&
         equal(c_cs1.learners_next(), c_cs2.learners_next()) &&
         c_cs1.auto_leave() == c_cs2.auto_leave();
}

}  // namespace craft