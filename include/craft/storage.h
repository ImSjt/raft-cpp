#ifndef __CRAFT_STORAGE_H__
#define __CRAFT_STORAGE_H__

#include <cstdint>
#include <string>
#include <vector>
#include <mutex>

#include "craft/noncopyable.h"
#include "craft/status.h"
#include "craft/raft.pb.h"

namespace craft {
// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
const char* const kErrCompacted = "requested index is unavailable due to compaction";

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
const char* const kErrSnapOutOfDate = "requested index is older than the existing snapshot";

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
const char* const kErrUnavailable = "requested entry at index is unavailable";

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
const char* const kErrSnapshotTemporarilyUnavailable = "snapshot is temporarily unavailable";

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
class Storage : public noncopyable {
 public:
    virtual ~Storage() {}
    // InitialState returns the saved HardState and ConfState information.
    virtual Status InitialState(raftpb::HardState& hard_state, raftpb::ConfState& conf_state) const = 0;
    // SetHardState saves the current HardState.
    virtual Status SetHardState(const raftpb::HardState& st) = 0;
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
    virtual Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size, std::vector<raftpb::Entry>& ents) = 0;
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
    virtual Status Term(uint64_t i, uint64_t& term) = 0;
	// LastIndex returns the index of the last entry in the log.
    virtual uint64_t LastIndex() = 0;
    // FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
    virtual uint64_t FirstIndex() = 0;
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
    virtual Status SnapShot(raftpb::Snapshot& snapshot) = 0;
    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    virtual Status ApplySnapshot(const raftpb::Snapshot& snapshot) = 0;
    // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last ApplyConfChange must be passed in.
    virtual Status CreateSnapshot(uint64_t i, const raftpb::ConfState* cs, const std::string& data, raftpb::Snapshot& snapshot) = 0;
    // Compact discards all log entries prior to compactIndex.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    virtual Status Compact(uint64_t compact_index) = 0;
    // Append the new entries to storage.
    // entries[0].Index > ms.entries[0].Index
    virtual Status Append(std::vector<raftpb::Entry>& entries) = 0;
};

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class MemoryStorage : public Storage {
 public:
    MemoryStorage();
    virtual Status InitialState(raftpb::HardState& hard_state, raftpb::ConfState& conf_state) const override;
    virtual Status SetHardState(const raftpb::HardState& st) override;
    virtual Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size, std::vector<raftpb::Entry>& ents) override;
    virtual Status Term(uint64_t i, uint64_t& term) override;
    virtual uint64_t LastIndex() override;
    virtual uint64_t FirstIndex() override;
    virtual Status SnapShot(raftpb::Snapshot& snapshot) override;
    virtual Status ApplySnapshot(const raftpb::Snapshot& snapshot) override;
    virtual Status CreateSnapshot(uint64_t i, const raftpb::ConfState* cs, const std::string& data, raftpb::Snapshot& snapshot) override;
    virtual Status Compact(uint64_t compact_index) override;
    virtual Status Append(std::vector<raftpb::Entry>& entries) override;

 private:
    uint64_t LastIndexUnSafe() const;
    uint64_t FirstIndexUnSafe() const;

 private:
    // Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft thread, but Append() is run on an application
	// thread.
    std::mutex mutex_;
    raftpb::HardState hard_state_;
    raftpb::Snapshot snapshot_;
    // ents[i] has raft log position i+snapshot.Metadata.Index
    std::vector<raftpb::Entry> ents_;
};

} // namespace craft

#endif // __CRAFT_RAFT_STORAGE_H__