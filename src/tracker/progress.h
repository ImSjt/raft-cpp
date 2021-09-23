#ifndef __CRAFT_TRACKER_PROGRESS_H__
#define __CRAFT_TRACKER_PROGRESS_H__

#include <cstdint>
#include <memory>

#include "craft/noncopyable.h"
#include "tracker/inflights.h"

namespace craft {

enum StateType {
    kStateProbe,
    kStateReplicate,
    kStateSnapshot
};

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
class Progress : public noncopyable {
 public:
    Progress();

    // ResetState moves the Progress into the specified State, resetting ProbeSent,
    // PendingSnapshot, and Inflights.
    void ResetState(StateType state);

    // ProbeAcked is called when this peer has accepted an append. It resets
    // ProbeSent to signal that additional append messages should be sent without
    // further delay.
    void ProbeAcked() {
        probe_sent_ = false;
    }

    // BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
    // optionally and if larger, the index of the pending snapshot.
    void BecomeProbe();

    // BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
    void BecomeReplicate();

    // BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
    // snapshot index.
    void BecomeSnapshot(uint64_t snapshoti);

    // MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
    // index acked by it. The method returns false if the given n index comes from
    // an outdated message. Otherwise it updates the progress and returns true.
    bool MaybeUpdate(uint64_t n);

     // OptimisticUpdate signals that appends all the way up to and including index n
    // are in-flight. As a result, Next is increased to n+1.
    void OptimisticUpdate(uint64_t n) {
        next_ = n + 1;
    }

    // MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
    // arguments are the index of the append message rejected by the follower, and
    // the hint that we want to decrease to.
    //
    // Rejections can happen spuriously as messages are sent out of order or
    // duplicated. In such cases, the rejection pertains to an index that the
    // Progress already knows were previously acknowledged, and false is returned
    // without changing the Progress.
    //
    // If the rejection is genuine, Next is lowered sensibly, and the Progress is
    // cleared for sending log entries.
    bool MaybeDecrTo(uint64_t rejected, uint64_t match_hint);

    // IsPaused returns whether sending log entries to this node has been throttled.
    // This is done when a node has rejected recent MsgApps, is currently waiting
    // for a snapshot, or has reached the MaxInflightMsgs limit. In normal
    // operation, this is false. A throttled node will be contacted less frequently
    // until it has reached a state in which it's able to accept a steady stream of
    // log entries again.
    bool IsPaused();

    uint64_t Match() {
        return match_;
    }

    uint64_t Next() {
        return next_;
    }

    StateType State() {
        return state_;
    }

    uint64_t PendingSnapshot() {
        return pending_snapshot_;
    }

    bool RecentActive() {
        return recent_active_;
    }

    bool ProbeSent() {
        return probe_sent_;
    }

    bool IsLearner() {
        return is_learner_;
    }

 private:
    uint64_t match_, next_;

	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
    StateType state_;

	// PendingSnapshot is used in StateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
    uint64_t pending_snapshot_;

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//
	// TODO(tbg): the leader should always have this set to true.
    bool recent_active_;

	// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
	// true, raft should pause sending replication message to this peer until
	// ProbeSent is reset. See ProbeAcked() and IsPaused().
    bool probe_sent_;

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
    std::unique_ptr<Inflights> inflights_;

    // IsLearner is true if this progress is tracked for a learner.
    bool is_learner_;
};

} // namespace craft 

#endif // __CRAFT_TRACKER_PROGRESS_H__