#ifndef __CRAFT_TRACKER_TRACKER_H__
#define __CRAFT_TRACKER_TRACKER_H__

#include <cstdint>
#include <vector>
#include <set>
#include <map>
#include <memory>
#include <functional>

#include "craft/noncopyable.h"
#include "craft/raft.pb.h"
#include "quorum/joint.h"
#include "tracker/progress.h"

namespace craft {

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
class ProgressTracker : public noncopyable {    
 public:
    // Config reflects the configuration tracked in a ProgressTracker.
    struct Config {
        JointConfig voters_;
        // AutoLeave is true if the configuration is joint and a transition to the
        // incoming configuration should be carried out automatically by Raft when
        // this is possible. If false, the configuration will be joint until the
        // application initiates the transition manually.
        bool auto_leave_;
        // Learners is a set of IDs corresponding to the learners active in the
        // current configuration.
        //
        // Invariant: Learners and Voters does not intersect, i.e. if a peer is in
        // either half of the joint config, it can't be a learner; if it is a
        // learner it can't be in either half of the joint config. This invariant
        // simplifies the implementation since it allows peers to have clarity about
        // its current role without taking into account joint consensus.
        std::set<uint64_t> learners_;
        // When we turn a voter into a learner during a joint consensus transition,
        // we cannot add the learner directly when entering the joint state. This is
        // because this would violate the invariant that the intersection of
        // voters and learners is empty. For example, assume a Voter is removed and
        // immediately re-added as a learner (or in other words, it is demoted):
        //
        // Initially, the configuration will be
        //
        //   voters:   {1 2 3}
        //   learners: {}
        //
        // and we want to demote 3. Entering the joint configuration, we naively get
        //
        //   voters:   {1 2} & {1 2 3}
        //   learners: {3}
        //
        // but this violates the invariant (3 is both voter and learner). Instead,
        // we get
        //
        //   voters:   {1 2} & {1 2 3}
        //   learners: {}
        //   next_learners: {3}
        //
        // Where 3 is now still purely a voter, but we are remembering the intention
        // to make it a learner upon transitioning into the final configuration:
        //
        //   voters:   {1 2}
        //   learners: {3}
        //   next_learners: {}
        //
        // Note that next_learners is not used while adding a learner that is not
        // also a voter in the joint config. In this case, the learner is added
        // right away when entering the joint configuration, so that it is caught up
        // as soon as possible.
        std::set<uint64_t> learners_next_;
    };

    using Closure = std::function<void (uint64_t id, std::shared_ptr<Progress> pr)>;

    ProgressTracker(int32_t max_inflight) : max_inflight_(max_inflight) {}

    // ConfState returns a ConfState representing the active configuration.
    raftpb::ConfState ConfState();

    // IsSingleton returns true if (and only if) there is only one voting member
    // (i.e. the leader) in the current configuration.
    bool IsSingleton() {
        return config_.voters_.At(0).Size() == 1 && config_.voters_.At(1).Size() == 0;
    }

    // Committed returns the largest log index known to be committed based on what
    // the voting members of the group have acknowledged.
    uint64_t Committed();

    // Visit invokes the supplied closure for all tracked progresses in stable order.
    void Visit(Closure&& func);

    // QuorumActive returns true if the quorum is active from the view of the local
    // raft state machine. Otherwise, it returns false.
    bool QuorumActive();

    // VoterNodes returns a sorted slice of voters.
    std::vector<uint64_t> VoterNodes();

    // LearnerNodes returns a sorted slice of learners.
    std::vector<uint64_t> LearnerNodes();

    // ResetVotes prepares for a new round of vote counting via recordVote.
    void ResetVotes() {
        votes_.clear();
    }

    // RecordVote records that the node with the given id voted for this Raft
    // instance if v == true (and declined it otherwise).
    void RecordVote(uint64_t id, bool v);

    // TallyVotes returns the number of granted and rejected Votes, and whether the
    // election outcome is known.
    VoteState TallyVotes(int32_t& granted, int32_t& rejected);

    Config& GetConfig() {
        return config_;
    }

    std::shared_ptr<Progress> GetProgress(uint64_t id);

 private:
    Config config_;
    std::map<uint64_t, std::shared_ptr<Progress>> progress_;
    std::map<uint64_t, bool> votes_;
    int32_t max_inflight_;
};

} // namespace craft

#endif // 