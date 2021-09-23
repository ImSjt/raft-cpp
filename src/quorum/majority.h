#ifndef __CRAFT_QUORUM_MAJORITY_H__
#define __CRAFT_QUORUM_MAJORITY_H__

#include <cstdint>
#include <string>
#include <vector>
#include <set>
#include <map>

#include "quorum/quorum.h"

namespace craft {

class JointConfig;

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
class MajorityConfig {
 public:
    MajorityConfig() = default;
    MajorityConfig(const std::set<uint64_t>& c) : majority_config_(c) {}
    MajorityConfig(std::set<uint64_t>&& c) : majority_config_(std::forward<std::set<uint64_t>>(c)) {}

    //  std::string String();

    // Describe returns a (multi-line) representation of the commit indexes for the
    // given lookuper.
    //  std::string Describe(AckedIndexer* l);

    size_t Size() {
        return majority_config_.size();
    }

    // Slice returns the MajorityConfig as a sorted slice.
    std::vector<uint64_t> Slice();

    // CommittedIndex computes the committed index from those supplied via the
    // provided AckedIndexer (for the active config).
    uint64_t CommittedIndex(AckedIndexer& l);
    uint64_t CommittedIndex(AckedIndexer&& l) {
        return CommittedIndex(l);
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending (i.e. neither a quorum of
    // yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    // quorum of no has been reached).
    VoteState VoteResult(const std::map<uint64_t, bool>& votes);

 private:
    friend class JointConfig;
    // The node set of the current configuration
    std::set<uint64_t> majority_config_;

};

} // namespace craft

#endif // __CRAFT_QUORUM_MAJORITY_H__