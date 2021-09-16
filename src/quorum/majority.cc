#include "quorum/majority.h"

#include <algorithm>

namespace craft {

std::string MajorityConfig::String() {
    std::vector<uint64_t> sl;

    for (uint64_t id : majority_config_) {
        sl.push_back(id);
    }

    std::sort(sl.begin(), sl.end(), [] (uint64_t i, uint64_t j) {
        return i < j;
    });

    std::string buf;
    buf.push_back('(');
    for (size_t i = 0; i < sl.size(); i++) {
        if (i > 0) {
            buf.push_back(' ');
        }
        buf.append(std::to_string(sl[i]));
    }
    buf.push_back(')');

    return buf;
}

std::string MajorityConfig::Describe(AckedIndexer* l) {
    if (majority_config_.empty()) {
        return "<empty majority quorum>";
    }

    struct tup
    {
        tup(uint64_t id, uint64_t idx, bool ok) : id_(id), idx_(idx), ok_(ok), bar_(0) {}
        uint64_t id_;
        uint64_t idx_;
        bool     ok_;  // idx found?
        int      bar_; // length of bar displayed for this tup
    };
    
	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.
    size_t n = majority_config_.size();
    std::vector<tup> info;

    uint64_t index;
    bool ok;
    for (uint64_t id : majority_config_) {
        ok = l->AckedIndex(id, index);
        info.push_back(tup(id, index, ok));
    }

    // Sort by index
    std::sort(info.begin(), info.end(), [] (const tup& i, const tup& j) {
        if (i.idx_ == j.idx_) {
            return i.id_ < j.id_;
        }

        return i.idx_ < j.idx_;
    });

    // Populate .bar.
    for (size_t i = 0; i < info.size(); i++) {
        if (i > 0 && info[i-1].idx_ < info[i].idx_) {
            info[i].bar_ = i;
        }
    }

    // Sort by ID.
    std::sort(info.begin(), info.end(), [] (const tup& i, const tup& j) {
        return i.idx_ < j.idx_;
    });

    // Print
    std::string buf;
    char tmp_buf[1024];
    int len;

    buf.append(n, ' ');
    buf.append("    idx\n");
    for (tup& t : info) {
        int bar = t.bar_;
        if (!t.ok_) {
            buf.append("?");
            buf.append(n, ' ');
        } else {
            buf.append(bar, 'x');
            buf.append(">");
            buf.append(n-bar, ' ');
        }
        
        len = snprintf(tmp_buf, sizeof(tmp_buf), " %5d    (id=%d)\n", t.idx_, t.id_);
        buf.append(tmp_buf, len);
    }

    return buf;
}

std::vector<uint64_t> MajorityConfig::Slice() {
    std::vector<uint64_t> sl;
    for (uint64_t id : majority_config_) {
        sl.push_back(id);
    }
    std::sort(sl.begin(), sl.end());

    return sl;
}

static void insertionSort(uint64_t* arr, size_t n) {
    size_t a = 0;
    size_t b = n;
    for (size_t i = a + 1; i < b; i++) {
        for (size_t j = i; j > a && arr[j] < arr[j-1]; j--) {
            std::swap(arr[j], arr[j-1]);
        }
    }
}

uint64_t MajorityConfig::CommittedIndex(AckedIndexer* l) {
    size_t n = majority_config_.size();
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
    if (sizeof(stk) >= n) {
        srt = stk;
    } else {
        srt = new uint64_t[n]();
    }

    {
		// Fill the slice with the indexes observed. Any unused slots will be
		// left as zero; these correspond to voters that may report in, but
		// haven't yet. We fill from the right (since the zeroes will end up on
		// the left after sorting below anyway).
        size_t i = n - 1;
        for (uint64_t id : majority_config_) {
            uint64_t idx;
            bool ok;
            ok = l->AckedIndex(id, idx);
            if (ok) {
                srt[i] = idx;
                i--;
            }
        }
    }

	// Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
	// package) to keep srt on the stack.
    insertionSort(srt, n);

    size_t pos = n - (n/2 + 1);
    uint64_t index = srt[pos];

    if (sizeof(stk) < n) {
        delete[] srt;
    }

    return index;
}

VoteState MajorityConfig::VoteResult(const std::map<uint64_t, bool>& votes) {
    if (majority_config_.empty()) {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
        return kVoteWon;
    }

    uint64_t ny[2] = {0}; // vote counts for no and yes, respectively

    int missing = 0;
    for (uint64_t id : majority_config_) {
        std::map<uint64_t, bool>::const_iterator it = votes.find(id);
        if (it == votes.end()) {
            missing++;
            continue;
        }

        if (it->second) {
            ny[1]++;
        } else {
            ny[0]++;
        }
    }

    size_t q = majority_config_.size()/2 + 1;
    if (ny[1] >= q) {
        return kVoteWon;
    }
    if (ny[1]+ missing >= q) {
        return kVotePending;
    }

    return kVoteLost;
}

} // namespace craft