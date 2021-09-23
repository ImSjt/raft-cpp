#include "tracker/progress.h"

#include <cassert>

#include "common/logger.h"

namespace craft {

Progress::Progress() :
    match_(0),
    next_(0),
    state_(kStateProbe),
    pending_snapshot_(0),
    recent_active_(false),
    probe_sent_(false),
    is_learner_(false) {

}

void Progress::ResetState(StateType state) {
    probe_sent_ = false;
    pending_snapshot_ = 0;
    state_ = state;

    assert(inflights_ != nullptr);
    inflights_->Reset();
}

void Progress::BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
    if (state_ == kStateSnapshot) {
        uint64_t pending_snapshot = pending_snapshot_;
        ResetState(kStateProbe);
        next_ = std::max<uint64_t>(match_+1, pending_snapshot+1);
    } else {
        ResetState(kStateProbe);
        next_ = match_ + 1;
    }
}

void Progress::BecomeReplicate() {
    ResetState(kStateReplicate);
    next_ = match_ + 1;
}

void Progress::BecomeSnapshot(uint64_t snapshoti) {
    ResetState(kStateSnapshot);
    pending_snapshot_ = snapshoti;
}

bool Progress::MaybeUpdate(uint64_t n) {
    bool updated = false;
    if (match_ < n) {
        match_ = n;
        updated = true;
        ProbeAcked();
    }
    next_ = std::max<uint64_t>(next_, n+1);
    return updated;
}

bool Progress::MaybeDecrTo(uint64_t rejected, uint64_t match_hint) {
    // The rejection must be stale if the progress has matched and "rejected"
    // is smaller than "match".
    if (state_ == kStateReplicate) {
        if (rejected <= match_) {
            return false;
        }

        // Directly decrease next to match + 1.
        //
        // TODO(tbg): why not use matchHint if it's larger?
        next_ = match_ + 1;
        return true;
    }

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
    if (next_-1 != rejected) {
        return false;
    }

    next_ = std::max<uint64_t>(std::min<uint64_t>(rejected, match_hint+1), 1);
    probe_sent_ = false;
    return true;
}

bool Progress::IsPaused() {
    switch (state_) {
    case kStateProbe:
        return probe_sent_;
    case kStateReplicate:
        assert(inflights_ != nullptr);
        return inflights_->Full();
    case kStateSnapshot:
        return true;
    default:
        LOG_FATAL("unexpected state");
    }
}

} // namespace craft