#include "quorum/joint.h"

namespace craft {

std::string JointConfig::String() {
    if (!joint_config_[1].majority_config_.empty()) {
        return joint_config_[0].String() + "&&" + joint_config_[1].String();
    }

    return joint_config_[0].String();
}

std::set<uint64_t> JointConfig::IDs() {
    std::set<uint64_t> m;
    for (const MajorityConfig& mc : joint_config_) {
        for (uint64_t id : mc.majority_config_) {
            m.insert(id);
        }
    }

    return m;
}

std::string JointConfig::Describe(AckedIndexer* l) {
    return MajorityConfig(IDs()).Describe(l);
}

uint64_t JointConfig::CommittedIndex(AckedIndexer* l) {
    uint64_t idx0 = joint_config_[0].CommittedIndex(l);
    uint64_t idx1 = joint_config_[1].CommittedIndex(l);
    if (idx0 < idx1) {
        return idx0;
    }
    return idx1;
}

VoteState JointConfig::VoteResult(const std::map<uint64_t, bool>& votes) {
    VoteState r1 = joint_config_[0].VoteResult(votes);
    VoteState r2 = joint_config_[1].VoteResult(votes);

    if (r1 == r2) {
        // If they agree, return the agreed state.
        return r1;
    }
    if (r1 == kVoteLost || r2 == kVoteLost) {
        // If either config has lost, loss is the only possible outcome.
        return kVoteLost;
    }
    // One side won, the other one is pending, so the whole outcome is.
    return kVotePending;
}

} // namespace craft
