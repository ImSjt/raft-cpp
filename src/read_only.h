#ifndef __CRAFT_READ_ONLY_H__
#define __CRAFT_READ_ONLY_H__

#include <cstdint>
#include <string>
#include <map>
#include <list>
#include <memory>

#include "craft/noncopyable.h"

namespace craft {

// ReadState provides state for read only query.
// It's caller's responsibility to call read_index_ first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through request_ctx_, eg. given a unique id as
// request_ctx_
struct ReadState {
    uint64_t index_;
    std::string request_ctx_;
};

struct ReadIndexStatus {
    // Message req_;
    uint64_t index_;
    std::map<uint64_t, bool> acks_;
};

class ReadOnly : public noncopyable {
 public:
    enum ReadOnlyOption {
        // kSafe guarantees the linearizability of the read only request by
	    // communicating with the quorum. It is the default and suggested option.
        kSafe,
        // kLeaseBased ensures linearizability of the read only request by
        // relying on the leader lease. It can be affected by clock drift.
        // If the clock drift is unbounded, leader might keep the lease longer than it
        // should (clock can move backward/pause without any bound). read_index_ is not safe
        // in that case.
        kLeaseBased,
        kNumReadOnlyOption
    };



 private:
    ReadOnlyOption option_;
    std::map<std::string, std::shared_ptr<ReadIndexStatus>> pending_read_index_;
    std::list<std::string> read_index_queue_;
};

} // namespace craft

#endif // __CRAFT_READ_ONLY_H__