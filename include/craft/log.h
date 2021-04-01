#ifndef __CRAFT_LOG_H__
#define __CRAFT_LOG_H__

#include <cstdio>
#include <string>
#include <memory>

#include "craft/noncopyable.h"
#include "craft/status.h"
#include "craft/log_unstable.h"
#include "craft/storage.h"

namespace craft {

class RaftLog : public noncopyable {
public:
    static const uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();
    // CreateNew returns log using the given storage and default options. It
    // recovers the log to the state that it just commits and applies the
    // latest snapshot.
    static RaftLog* CreateNew(std::shared_ptr<Storage>& storage) {
        return CreateNewWithSize(storage, kNoLimit);
    }

    // CreateNewWithSize returns a log using the given storage and max
    // message size.
    static RaftLog* CreateNewWithSize(std::shared_ptr<Storage>& storage, uint64_t max_next_ents_size) {
        return new RaftLog(storage, max_next_ents_size);
    }

    RaftLog(std::shared_ptr<Storage>& storage, uint64_t max_next_ents_size=kNoLimit);

    std::string String() const {
        char buf[1024];
        snprintf(buf, sizeof(buf), "committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
                                    committed_, applied_, unstable_.offset_, unstable_.entries_.size());

        return std::string(buf);
    }

    // MaybeAppend returns 0 if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries).
    uint64_t MaybeAppend(uint64_t index, uint64_t log_term, uint64_t committed, std::vector<raftpb::Entry>& ents);

    void UnstableEntries(std::vector<raftpb::Entry>& ents) const;
    // NextEnts returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    void NextEnts(std::vector<raftpb::Entry>& ents) const;
    // HasNextEnts returns if there is any available entries for execution. This
    // is a fast check without heavy raftLog.Slice() in raftLog.NextEnts().
    bool HasNextEnts() const;
    // HasPendingSnapshot returns if there is pending snapshot waiting for applying.
    bool HasPendingSnapshot() const;

    Status Snapshot(raftpb::Snapshot& snapshot) const;

    uint64_t FirstIndex() const;

    uint64_t LastIndex() const;

    void CommitTo(uint64_t tocommit); // private?

    void AppliedTo(uint64_t i);

    void StableTo(uint64_t i, uint64_t t) { unstable_.StableTo(i, t); }

    void StableSnapTo(uint64_t i) { unstable_.StableSnapTo(i); }

    uint64_t LastTerm() const;

    Status Term(uint64_t i, uint64_t& term) const;

    Status Entries(uint64_t i, uint64_t maxsize, std::vector<raftpb::Entry>& ents) const;

    // AllEntries returns all entries in the log.
    void AllEntries(std::vector<raftpb::Entry>& ents) const;
    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    bool IsUpToData(uint64_t lasti, uint64_t term) const;

    bool MatchTerm(uint64_t i, uint64_t term) const;

    bool MaybeCommit(uint64_t max_index, uint64_t term);

    void Restore(const raftpb::Snapshot& snapshot);

protected:
    uint64_t Append(const std::vector<raftpb::Entry>& ents);
    // FindConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    uint64_t FindConflict(const std::vector<raftpb::Entry>& ents) const;
    // Slice returns a slice of log entries from lo through hi-1, inclusive.
    Status Slice(uint64_t lo, uint64_t hi, uint64_t max_size, std::vector<raftpb::Entry>& ents) const;
    // firstIndex <= lo <= hi <= firstIndex + len(l.entries)
    Status MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

    uint64_t ZeroTermOnErrCompacted(uint64_t t, const Status& status) const;

private:
    // storage_ contains all stable entries since the last snapshot.
    std::shared_ptr<Storage> storage_;
	// unstable_ contains all unstable entries and snapshot.
	// they will be saved into storage.
    Unstable unstable_;
	// committed_ is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
    uint64_t committed_;
	// applied_ is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied_ <= committed_
    uint64_t applied_;
    // max_next_ents_size_ is the maximum number aggregate byte size of the messages
	// returned from calls to NextEnts.
    uint64_t max_next_ents_size_;
};

} // namespace craft

#endif // __CRAFT_LOG_H__