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
#include "src/quorum/majority.h"

#include <algorithm>
#include <memory>
#include <sstream>

namespace craft {

std::vector<uint64_t> MajorityConfig::Slice() const {
  std::vector<uint64_t> sl;
  for (uint64_t id : config_) {
    sl.push_back(id);
  }
  std::sort(sl.begin(), sl.end());
  return sl;
}

static void insertionSort(uint64_t* arr, size_t n) {
  size_t a = 0;
  size_t b = n;
  for (size_t i = a + 1; i < b; i++) {
    for (size_t j = i; j > a && arr[j] < arr[j - 1]; j--) {
      std::swap(arr[j], arr[j - 1]);
    }
  }
}

Index MajorityConfig::CommittedIndex(const AckedIndexer& l) const {
  size_t n = Size();
  if (n == 0) {
    // This plays well with joint quorums which, when one half is the zero
    // MajorityConfig, should behave like the other half.
    return UINT64_MAX;
  }

  // Use an on-stack slice to collect the committed indexes when n <= 7
  // (otherwise we alloc). The alternative is to stash a slice on
  // MajorityConfig, but this impairs usability (as is, MajorityConfig is just
  // a map, and that's nice). The assumption is that running with a
  // replication factor of >7 is rare, and in cases in which it happens
  // performance is a lesser concern (additionally the performance
  // implications of an allocation here are far from drastic).
  uint64_t stk[7] = {0};
  uint64_t* srt = nullptr;
  std::unique_ptr<uint64_t[]> guard;
  if (sizeof(stk) / sizeof(stk[0]) >= n) {
    srt = stk;
  } else {
    srt = new uint64_t[n]();
    guard.reset(srt);
  }

  {
    // Fill the slice with the indexes observed. Any unused slots will be
    // left as zero; these correspond to voters that may report in, but
    // haven't yet. We fill from the right (since the zeroes will end up on
    // the left after sorting below anyway).
    size_t i = n - 1;
    for (uint64_t id : config_) {
      auto [idx, ok] = l.AckedIndex(id);
      if (ok) {
        srt[i] = idx;
        i--;
      }
    }
  }

  // Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
  // package) to keep srt on the stack.
  insertionSort(srt, n);

	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
  size_t pos = n - (n / 2 + 1);
  return srt[pos];
}

VoteState MajorityConfig::VoteResult(const std::map<uint64_t, bool>& votes) const {
  if (config_.empty()) {
    // By convention, the elections on an empty config win. This comes in
    // handy with joint quorums because it'll make a half-populated joint
    // quorum behave like a majority quorum.
    return kVoteWon;
  }

  int voted_cnt = 0;  // vote counts for yes.
  int missing = 0;
  for (auto id : config_) {
    auto it = votes.find(id);
    if (it == votes.end()) {
      missing++;
      continue;
    }
    if (it->second) {
      voted_cnt++;
    }
  }

  size_t q = Size() / 2 + 1;
  if (voted_cnt >= static_cast<int>(q)) {
    return VoteState::kVoteWon;
  }
  if (voted_cnt + missing >= static_cast<int>(q)) {
    return VoteState::kVotePending;
  }
  return VoteState::kVoteLost;
}

void MajorityConfig::IDs(std::set<uint64_t>& ids) const {
  for (auto& id : config_) {
    ids.insert(id);
  }
}

std::string MajorityConfig::String() const {
  std::stringstream ss;
  auto sl = Slice();
  ss << "(";
  for (size_t i = 0; i < sl.size(); i++) {
    if (i > 0) {
      ss << " ";
    }
    ss << sl[i];
  }
  ss << ")";
  return ss.str();
}

std::string MajorityConfig::Describe(const AckedIndexer& l) const {
  return "";
}

}  // namespace craft