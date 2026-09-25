#pragma once
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace djq {

// Tracks currently-connected workers, which one is idle vs. busy, and which
// job_id (if any) a busy worker is currently holding. Round-robin alone
// isn't enough: with one worker, blind rotation would keep handing it more
// jobs while it's still busy, and on failure the coordinator needs to know
// exactly which job to reclaim (Section 6).
class WorkerPool {
public:
    void add(uint64_t worker_id, int socket_fd);

    // Removes the worker. If it was holding an undelivered-result job,
    // returns that job's id so the caller can reassign it — this is the
    // actual "detected failure -> reclaim in-flight work" hookup.
    std::optional<uint64_t> remove(uint64_t worker_id);

    bool has_idle() const;

    // Picks the next idle worker in round-robin order and marks it busy.
    // Caller must check has_idle() first.
    uint64_t next();

    int fd_of(uint64_t worker_id) const;

    // Records that `worker_id` now holds `job_id` (called right after a
    // successful dispatch).
    void assign_job(uint64_t worker_id, uint64_t job_id);

    // Called once a JOB_RESULT comes back — frees the worker and clears
    // its held job.
    void mark_idle(uint64_t worker_id);

private:
    struct WorkerInfo {
        int socket_fd;
        bool busy = false;
        std::optional<uint64_t> current_job;
    };
    std::unordered_map<uint64_t, WorkerInfo> workers_;
    std::vector<uint64_t> order_;  // registration order, cycled through
    size_t cursor_ = 0;
};

} // namespace djq
