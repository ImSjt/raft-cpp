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
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "gtest/gtest.h"
#include "tracker/inflights.h"

static craft::Inflights makeInflights(int32_t start, int32_t count,
                                      int32_t size, std::vector<uint64_t> buffer) {
  craft::Inflights in(size);
  in.SetStrat(start);
  in.SetCount(count);
  in.SetBuffer(buffer);
  return in;
}

TEST(Inflights, Add) {
	// no rotating case
  craft::Inflights in(10);
  for (size_t i = 0; i < 10; i++) {
    in.Add(i);
  }

  in.FreeLE(4);

  {
    //                                                     ↓------------
    auto win = makeInflights(5, 5, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  in.FreeLE(8);

  {
    //                                                             ↓
    auto win = makeInflights(9, 1, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  // rotating case
  for (uint64_t i = 10; i < 15; i++) {
    in.Add(i);
  }

  in.FreeLE(12);

  {
    //                                              ↓-----
    auto win = makeInflights(3, 2, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  in.FreeLE(14);

  {
    //                                  ↓-----
    auto win = makeInflights(0, 0, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }
}

TEST(Inflights, FreeLE) {
  // no rotating case
  craft::Inflights in(10);
  for (uint64_t i = 0; i < 10; i++) {
    in.Add(i);
  }

  in.FreeLE(4);

  {
    //                                                 ↓------------
    auto win = makeInflights(5, 5, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  in.FreeLE(8);

  {
    //                                                             ↓
    auto win = makeInflights(9, 1, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  // rotating case
  for (uint64_t i = 10; i < 15; i++) {
    in.Add(i);
  }

  in.FreeLE(12);

  {
    //                                              ↓-----
    auto win = makeInflights(3, 2, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }

  in.FreeLE(14);
  {
    //                                              ↓-----
    auto win = makeInflights(0, 0, 10, {10, 11, 12, 13, 14, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }
}

TEST(Inflights, FreeFirstOne) {
  craft::Inflights in(10);
  for (uint64_t i = 0; i < 10; i++) {
    in.Add(i);
  }

  in.FreeFirstOne();

  {
    //                                     ↓------------------------
    auto win = makeInflights(1, 9, 10, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ASSERT_EQ(in.Start(), win.Start());
    ASSERT_EQ(in.Count(), win.Count());
    ASSERT_EQ(in.Size(), win.Size());
    ASSERT_EQ(in.Buffer(), win.Buffer());
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}