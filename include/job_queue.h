#pragma once
#include "job.h"
#include <cstdint>
#include <optional>
#include <queue>
#include <unordered_map>
#include <vector>

namespace djq {

// Not internally thread-safe, and deliberately stays that way (Section 7):
// the coordinator's SharedState wraps one mutex around this, WorkerPool, and
// the WAL together, since dispatch already treats all three as one atomic
// unit. Callers outside coordinator/main.cpp must provide their own external
// synchronization before touching a shared instance concurrently.
class JobQueue {
public:
    static constexpr int MAX_RETRIES = 3;

    // Creates a new job, assigns it an ID, enqueues it, returns the ID.
    uint64_t submit(std::string payload, int priority);

    // Pops the highest-priority PENDING job (FIFO among equal priorities),
    // marks it DISPATCHED, and returns a copy. Empty if nothing is pending.
    std::optional<Job> try_dispatch();

    // Pointer into the source-of-truth map; nullptr if job_id is unknown.
    // Not stable across calls that touch jobs_ (map rehash) — copy out what you need.
    Job* find(uint64_t job_id);

    void mark_succeeded(uint64_t job_id);

    // Bumps retry_count; re-enqueues as PENDING if under MAX_RETRIES,
    // otherwise marks FAILED permanently. Same path used for real failures
    // and for jobs reclaimed from a dead worker (Section 6).
    void mark_failed_or_retry(uint64_t job_id);

    size_t pending_count() const { return order_.size(); }

    // --- WAL replay only (see WriteAheadLog::replay) ---
    // Re-creates a job with a KNOWN id from a logged SUBMITTED record,
    // instead of assigning a fresh one, and advances next_id_ past it so
    // post-recovery submissions never collide with a replayed id.
    void replay_submit(uint64_t job_id, std::string payload, int priority);

    // Sets status directly to DISPATCHED without popping from the ordering
    // heap (there's no "worker" during replay to hand it to) — see
    // requeue_orphaned_dispatched_jobs() for how these get reconciled.
    void replay_dispatched(uint64_t job_id);

    // A job still DISPATCHED after replay finishes was, by definition, in
    // flight when the coordinator crashed — its worker's TCP connection is
    // gone, so it can never report back. Puts every such job back to
    // PENDING (without touching retry_count) so it gets redispatched.
    void requeue_orphaned_dispatched_jobs();

private:
    struct QueueEntry {
        int priority;
        uint64_t seq;      // tiebreaker so equal-priority jobs stay FIFO, not arbitrary
        uint64_t job_id;
    };
    struct Compare {
        // std::priority_queue is a max-heap; this returns true when `a` should
        // rank BELOW `b`, i.e. higher priority first, then lower seq (older) first.
        bool operator()(const QueueEntry& a, const QueueEntry& b) const {
            if (a.priority != b.priority) return a.priority < b.priority;
            return a.seq > b.seq;
        }
    };

    void enqueue(const Job& job);

    std::priority_queue<QueueEntry, std::vector<QueueEntry>, Compare> order_;
    std::unordered_map<uint64_t, Job> jobs_;
    uint64_t next_id_ = 1;
    uint64_t next_seq_ = 0;
};

} // namespace djq
