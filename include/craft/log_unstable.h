#ifndef __CRAFT_LOG_UNSTABLE_H__
#define __CRAFT_LOG_UNSTABLE_H__

#include <memory>
#include <vector>

#include "craft/raft.pb.h"
#include "craft/noncopyable.h"
#include "craft/status.h"

class RaftLog;
namespace craft {
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
class Unstable : public noncopyable {
 public:
    Unstable() : offset_(0) {}
    // maybeFirstIndex returns the index of the first possible entry in entries
    // if it has a snapshot.
    uint64_t MaybeFirstIndex() const;
    // maybeLastIndex returns the last index if it has at least one
    // unstable entry or snapshot.
    uint64_t MaybeLastIndex() const;
    // maybeTerm returns the term of the entry at index i, if there
    // is any.
    uint64_t MaybeTerm(uint64_t i) const;

    void StableTo(uint64_t i, uint64_t t);

    void StableSnapTo(uint64_t i);

    void Restore(const raftpb::Snapshot& snapshot);

    void TruncateAndAppend(const std::vector<raftpb::Entry>& ents);

    void Slice(uint64_t lo, uint64_t hi, std::vector<raftpb::Entry>& ents) const;

 private:
    void ShrikEntriesArray();

    void MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

 private:
    friend class RaftLog;
    std::unique_ptr<raftpb::Snapshot> snapshot_;
    std::vector<raftpb::Entry> entries_;
    uint64_t offset_;
};

} // namespace craft

#endif // __CRAFT_RAFT_LOG_UNSTABLE_H__