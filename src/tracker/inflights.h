#ifndef __CRAFT_TRACKER_INFLIGHTS_H__
#define __CRAFT_TRACKER_INFLIGHTS_H__

#include <cstdint>
#include <vector>
#include <memory>

namespace craft {

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
class Inflights {
 public:
    Inflights(int32_t size) : start_(0), count_(0), size_(size) {}

    // Clone returns an *Inflights that is identical to but shares no memory with
    // the receiver.
    // std::unique_ptr<Inflights> Clone();

    // Add notifies the Inflights that a new message with the given index is being
    // dispatched. Full() must be called prior to Add() to verify that there is room
    // for one more message, and consecutive calls to add Add() must provide a
    // monotonic sequence of indexes.
    void Add(uint64_t inflight);

    // FreeLE frees the inflights smaller or equal to the given `to` flight.
    void FreeLE(uint64_t to);

    // FreeFirstOne releases the first inflight. This is a no-op if nothing is
    // inflight.
    void FreeFirstOne();

    // Full returns true if no more messages can be sent at the moment.
    bool Full() {
        return count_ == size_;
    }

    // Count returns the number of inflight messages.
    int32_t Count() {
        return count_;
    }

    // reset frees all inflights.
    void Reset() {
        count_ = 0;
        start_ = 0;
    }

 private:
    // grow the inflight buffer by doubling up to inflights.size. We grow on demand
    // instead of preallocating to inflights.size to handle systems which have
    // thousands of Raft groups per process.
    void Grow();

 private:
    // the starting index in the buffer
    int32_t start_;

    // number of inflights in the buffer
    int32_t count_;

    // the size of the buffer
    int32_t size_;

	// buffer contains the index of the last entry
	// inside one message.
    std::vector<uint64_t> buffer_;
};

} // namespace craft

#endif // __CRAFT_TRACKER_INFLIGHTS_H__