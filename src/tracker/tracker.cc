#include "tracker/tracker.h"

#include "quorum/quorum.h"

namespace craft {

class MatchAckIndexer : public AckedIndexer {
 public:
    MatchAckIndexer(const std::map<uint64_t, std::shared_ptr<Progress>>& m) : progress_map_(m) {}
    MatchAckIndexer(std::map<uint64_t, std::shared_ptr<Progress>>&& m) : progress_map_(m) {}

    virtual bool AckedIndex(uint64_t voter_id, uint64_t& idx) override {
        auto it = progress_map_.find(voter_id);
        if (it == progress_map_.end()) {
            idx = 0;
            return false;
        }

        idx = it->second->Match();
        return true;
    }

 private:
    std::map<uint64_t, std::shared_ptr<Progress>> progress_map_;
};

raftpb::ConfState ProgressTracker::ConfState() {
    raftpb::ConfState conf_state;
    
    std::vector<uint64_t> voters = config_.voters_.At(0).Slice();
    for (uint64_t voter : voters) {
        conf_state.add_voters(voter);
    }

    std::vector<uint64_t> voters_outgoing = config_.voters_.At(1).Slice();
    for (uint64_t voter : voters_outgoing) {
        conf_state.add_voters_outgoing(voter);
    }

    for (uint64_t learner : config_.learners_) {
        conf_state.add_learners(learner);
    }

    for (uint64_t learner : config_.learners_next_) {
        conf_state.add_learners_next(learner);
    }

    conf_state.set_auto_leave(config_.auto_leave_);

    return conf_state;
}

uint64_t ProgressTracker::Committed() {
    return config_.voters_.CommittedIndex(MatchAckIndexer(progress_));
}

void ProgressTracker::Visit(Closure&& func) {
    for (const auto& p : progress_) {
        func(p.first, p.second);
    }
}

bool ProgressTracker::QuorumActive() {
    std::map<uint64_t, bool> votes;
    Visit([&](uint64_t id, std::shared_ptr<Progress> pr) {
        if (pr->IsLearner()) {
            return;
        }
        votes[id] = pr->RecentActive();
    });

    return config_.voters_.VoteResult(votes) == kVoteWon;
}

std::vector<uint64_t> ProgressTracker::VoterNodes() {
    std::set<uint64_t> m = config_.voters_.IDs();
    std::vector<uint64_t> nodes;
    for (uint64_t n : m) {
        nodes.push_back(n);
    }

    return nodes;
}

std::vector<uint64_t> ProgressTracker::LearnerNodes() {
    if (config_.learners_.empty()) {
        return {};
    }

    std::vector<uint64_t> nodes;
    for (uint64_t n : config_.learners_) {
        nodes.push_back(n);
    }
    
    return nodes;
}

void ProgressTracker::RecordVote(uint64_t id, bool v) {
    auto it = votes_.find(id);
    if (it == votes_.end()) {
        votes_[id] = v;
    }
}

VoteState ProgressTracker::TallyVotes(int32_t& granted, int32_t& rejected) {
	// Make sure to populate granted/rejected correctly even if the Votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
    granted = 0;
    rejected = 0;
    for (const auto& p : progress_) {
        if (p.second->IsLearner()) {
            continue;
        }
        auto v = votes_.find(p.first);
        if (v == votes_.end()) {
            continue;
        }
        if (v->second) {
            granted++;
        } else {
            rejected++;
        }
    }

    return config_.voters_.VoteResult(votes_);
}

std::shared_ptr<Progress> ProgressTracker::GetProgress(uint64_t id) {
    auto it = progress_.find(id);
    if (it == progress_.end()) {
        return std::make_shared<Progress>();
    }

    return it->second;
}

} // namespace craft