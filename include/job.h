#pragma once
#include <cstdint>
#include <string>

namespace djq {

enum class JobStatus : uint8_t {
    PENDING    = 0,  // sitting in the queue, not yet handed to a worker
    DISPATCHED = 1,  // sent to a worker, awaiting a result
    SUCCEEDED  = 2,
    FAILED     = 3,  // exhausted its retry budget
};

struct Job {
    uint64_t job_id = 0;
    std::string payload;      // the work itself — a shell command, run by the worker via popen()
    int priority = 0;         // higher runs sooner
    int retry_count = 0;      // how many times this job has been re-dispatched after a failure
    JobStatus status = JobStatus::PENDING;
};

} // namespace djq
