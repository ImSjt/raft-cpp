#ifndef __CRAFT_RAFT_RAFT_H__
#define __CRAFT_RAFT_RAFT_H__

#include <cstdint>
#include <memory>
#include <vector>

#include "craft/noncopyable.h"
#include "raft/read_only.h"

namespace craft {

class Raft : public noncopyable {
 public:
    enum StateType {
        kFollower,
        kCandidate,
        kLeader,
        kPreCandidate,
        kNumState
    };

 private:
    uint64_t id_;
    
    uint64_t term_;
    uint64_t vote_;

    std::vector<ReadState> read_states_;

    // std::unique_ptr<RaftLog> raft_log_ 

    // ProgressTracker prs_;

    StateType state_;

    // is_leader_ is true if the local raft node is a learner.
    bool is_leader_;

    // std::vector<Message> msgs;

    // the leader id
    uint64_t lead_;

    // lead_transferee_ is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
    uint64_t lead_transferee_;

    // Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pending_conf_index_, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
    uint64_t pending_conf_index_;

    // an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
    uint64_t uncommitted_size_;

    // std::unique_ptr<ReadOnly> read_only_;

	// number of ticks since it reached last election_elapsed_ when it is leader
	// or candidate.
	// number of ticks since it reached last election_elapsed_ or received a
	// valid message from current leader when it is a follower.
    int32_t election_elapsed_;

	// number of ticks since it reached last hearbeat_elapsed_.
	// only leader keeps hearbeat_elapsed_.
    int32_t hearbeat_elapsed_;

    bool check_quorum_;
    bool pre_vote_;

    int32_t heartbeat_timeout_;
    int32_t election_timeout_;

	// randomized_election_timeout_ is a random number between
	// [election_timeout_, 2 * election_timeout_ - 1]. It gets reset
	// when raft changes its state to follower or candidate.
    int32_t randomized_election_timeout_;
    bool disable_proposal_forwarding_;
};

} // namespace craft

#endif // __CRAFT_RAFT_RAFT_H__