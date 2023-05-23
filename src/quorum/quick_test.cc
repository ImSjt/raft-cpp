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
#include <vector>
#include <set>
#include <map>
#include <memory>
#include <random>

#include "gtest/gtest.h"
#include "src/quorum/majority.h"
#include "src/util.h"

static std::vector<int> makeRandomArray(int n) {
  std::vector<int> m(n);
  for (int i = 0; i < n; i++) {
    int j = craft::Util::Random(0, i);
    m[i] = m[j];
    m[j] = i;
  }
  return m;
}

// This is an alternative implementation of (MajorityConfig).CommittedIndex(l).
static craft::Index alternativeMajorityCommittedIndex(
    const craft::MajorityConfig& c, const craft::AckedIndexer& l) {
  if (c.Size() == 0) {
    return UINT64_MAX;
  }

  std::map<uint64_t, craft::Index> id_to_idx;
  for (auto id : c.Slice()) {
    auto [idx, ok] = l.AckedIndex(id);
    if (ok) {
      id_to_idx[id] = idx;
    }
  }

	// Build a map from index to voters who have acked that or any higher index.
  std::map<craft::Index, int> idx_to_votes;
  for (auto& p : id_to_idx) {
    idx_to_votes[p.second] = 0;
  }

  for (auto& p1 : id_to_idx) {
    for (auto& p2 : idx_to_votes) {
      if (p2.first > p1.second) {
        continue;
      }
      p2.second++;
    }
  }

	// Find the maximum index that has achieved quorum.
  auto q = c.Size() / 2 + 1;
  craft::Index max_quorum_idx = 0;
  for (auto& p : idx_to_votes) {
    if (p.second >= static_cast<int>(q) && p.first > max_quorum_idx) {
      max_quorum_idx = p.first;
    }
  }
  return max_quorum_idx;
}

// smallRandIdxMap returns a reasonably sized map of ids to commit indexes.
static std::map<uint64_t, craft::Index> smallRandIdxMap() {
  // std::map<uint64_t, craft::Index>();
  int size = 10;
  int n = craft::Util::Random(0, size);
  auto ids = makeRandomArray(2 * n);
  ids.erase(ids.begin() + n, ids.end());
  std::vector<int> idxs(ids.size());
  for (size_t i = 0; i < idxs.size(); i++) {
    idxs[i] = craft::Util::Random(0, n);
  }

  std::map<uint64_t, craft::Index> m;
  for (size_t i = 0; i < ids.size(); i++) {
    m[ids[i]] = static_cast<craft::Index>(idxs[i]);
  }
  return m;
}

static craft::MajorityConfig makeMajorityConfig(const std::map<uint64_t, craft::Index>& m) {
  std::set<uint64_t> s;
  for (auto& p : m) {
    s.insert(p.first);
  }
  return craft::MajorityConfig(std::move(s));
}

static craft::MapAckIndexer makeMapAckIndexer(std::map<uint64_t, craft::Index>& m) {
  return craft::MapAckIndexer(m);
}

TEST(Majority, Quick) {
  auto fn1 = [](const craft::MajorityConfig& c, const craft::AckedIndexer& l) {
    return c.CommittedIndex(l);
  };
  auto fn2 = [](const craft::MajorityConfig& c, const craft::AckedIndexer& l) {
    return alternativeMajorityCommittedIndex(c, l);
  };

  for (size_t i = 0; i < 50000; i++) {
    auto m1 = smallRandIdxMap();
    auto m2 = smallRandIdxMap();
    auto mj = makeMajorityConfig(m1);
    auto ai = makeMapAckIndexer(m2);
    ASSERT_EQ(fn1(mj, ai), fn2(mj, ai));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}