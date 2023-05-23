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
#include "gtest/gtest.h"
#include "src/raftpb/confstate.h"

static auto makeRepeatedField(std::vector<uint64_t> arr) {
  ::google::protobuf::RepeatedField< ::google::protobuf::uint64 > m;
  for (auto i : arr) {
    m.Add(i);
  }
  return m;
}

TEST(ConfState, Equivalent) {
  using namespace craft;
  // Reordered voters and learners.
  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 2, 3});
    *cs.mutable_learners() = makeRepeatedField({5, 4, 6});
    *cs.mutable_voters_outgoing() = makeRepeatedField({9, 8, 7});
    *cs.mutable_learners_next() = makeRepeatedField({10, 20, 15});

    raftpb::ConfState cs2;
    *cs2.mutable_voters() = makeRepeatedField({1, 2, 3});
    *cs2.mutable_learners() = makeRepeatedField({4, 5, 6});
    *cs2.mutable_voters_outgoing() = makeRepeatedField({7, 9, 8});
    *cs2.mutable_learners_next() = makeRepeatedField({20, 10, 15});
    ASSERT_TRUE(cs == cs2);
  }

  // Not sensitive to nil vs empty slice.
  {
    raftpb::ConfState cs;
    raftpb::ConfState cs2;
    ASSERT_TRUE(cs == cs2);
  }

  // Non-equivalent voters.
  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 2, 3, 4});
    raftpb::ConfState cs2;
    *cs2.mutable_voters() = makeRepeatedField({2, 1, 3});
    ASSERT_FALSE(cs == cs2);
  }
  {
    raftpb::ConfState cs;
    *cs.mutable_voters() = makeRepeatedField({1, 4, 3});
    raftpb::ConfState cs2;
    *cs2.mutable_voters() = makeRepeatedField({2, 1, 3});
    ASSERT_FALSE(cs == cs2);
  }

  // Non-equivalent learners.
  {
    raftpb::ConfState cs;
    *cs.mutable_learners() = makeRepeatedField({1, 4, 3});
    raftpb::ConfState cs2;
    *cs2.mutable_learners() = makeRepeatedField({2, 1, 3});
    ASSERT_FALSE(cs == cs2);
  }

  // Sensitive to AutoLeave flag.
  {
    raftpb::ConfState cs;
    cs.set_auto_leave(true);
    raftpb::ConfState cs2;
    ASSERT_FALSE(cs == cs2);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}