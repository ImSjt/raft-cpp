#ifndef __CRAFT_QUORUM_QUORUM_H__
#define __CRAFT_QUORUM_QUORUM_H__

#include <cstdint>
#include <map>

namespace craft {

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
class AckedIndexer {
 public:
    virtual bool AckedIndex(uint64_t voter_id, uint64_t& idx) = 0;
};

class MapAckIndexer : public AckedIndexer {
 public:
    MapAckIndexer(const std::map<uint64_t, uint64_t>& indexer) : map_ack_indexer_(indexer) {}
    MapAckIndexer(std::map<uint64_t, uint64_t>&& indexer) : map_ack_indexer_(indexer) {}

    virtual bool AckedIndex(uint64_t vote_id, uint64_t& idx) override {
        std::map<uint64_t, uint64_t>::iterator it = map_ack_indexer_.find(vote_id);
        if (it == map_ack_indexer_.end()) {
            idx = 0;
            return false;
        }

        idx = it->second;
        return true;
    }

 private:
    // voter_id -> commit_index
    std::map<uint64_t, uint64_t> map_ack_indexer_;

};

enum VoteState {
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
    kVotePending,
    // VoteLost indicates that the quorum has voted "no".
    kVoteLost,
    // VoteWon indicates that the quorum has voted "yes".
    kVoteWon
};

// std::string Index2Str();
// std::string VoteResult2Str();

} // namespace craft

#endif // __CRAFT_QUORUM_QUORUM_H__