#ifndef __CRAFT_QUORUM_JOINT_H__
#define __CRAFT_QUORUM_JOINT_H__

#include "quorum/majority.h"

#include <string>
#include <set>
#include <map>

namespace craft {

// JointConfig is a configuration of two groups of (possibly overlapping)
// majority configurations. Decisions require the support of both majorities.
class JointConfig {
 public:
   JointConfig() = default;
   JointConfig(const std::set<uint64_t>& c1, const std::set<uint64_t>& c2) : joint_config_{c1, c2} {}
   JointConfig(std::set<uint64_t>&& c1, std::set<uint64_t>&& c2) : joint_config_{std::forward<std::set<uint64_t>>(c1), std::forward<std::set<uint64_t>>(c2)} {}

   // MajorityConfig& operator[] (int i) {
   //    if (i < 0 || i >= sizeof(joint_config_)) {
   //       return joint_config_[0];
   //    }

   //    return joint_config_[i];
   // }

   MajorityConfig& At(size_t i) {
      if (i < 0 || i >= sizeof(joint_config_)) {
         return joint_config_[0];
      }

      return joint_config_[i];
   }

   //  std::string String();

    // IDs returns a newly initialized map representing the set of voters present
    // in the joint configuration.
    std::set<uint64_t> IDs();

    // Describe returns a (multi-line) representation of the commit indexes for the
    // given lookuper.
   //  std::string Describe(AckedIndexer* l);

    // CommittedIndex returns the largest committed index for the given joint
    // quorum. An index is jointly committed if it is committed in both constituent
    // majorities.
    uint64_t CommittedIndex(AckedIndexer& l);
    uint64_t CommittedIndex(AckedIndexer&& l) {
       return CommittedIndex(l);
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending, lost, or won. A joint quorum
    // requires both majority quorums to vote in favor.
    VoteState VoteResult(const std::map<uint64_t, bool>& votes);

 private:
    MajorityConfig joint_config_[2];
};

} // namespace craft

#endif // __CRAFT_QUORUM_JOINT_H__