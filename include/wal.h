#pragma once
#include "job_queue.h"
#include <cstdint>
#include <string>
#include <vector>

namespace djq {

enum class WalRecordType : uint8_t {
    SUBMITTED       = 1,
    DISPATCHED      = 2,
    SUCCEEDED       = 3,
    FAILED_OR_RETRY = 4,
};

// Append-only, fsync'd-per-write log of every state-changing JobQueue
// operation. On restart, replay() rebuilds a JobQueue from this file, so a
// coordinator crash never silently loses accepted work.
class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& path);
    ~WriteAheadLog();

    void log_submitted(uint64_t job_id, int priority, const std::string& payload);
    void log_dispatched(uint64_t job_id);
    void log_succeeded(uint64_t job_id);
    void log_failed_or_retry(uint64_t job_id);

    // Re-applies every record in the file, in order, to `queue`. A record
    // truncated mid-write (crash during that exact append) is detected and
    // discarded rather than corrupting replay — that operation never made
    // it far enough to have been acknowledged to anyone anyway.
    void replay(JobQueue& queue);

private:
    void append(WalRecordType type, const std::vector<uint8_t>& payload);
    int fd_;
};

} // namespace djq
