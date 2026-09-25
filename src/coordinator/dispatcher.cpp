#include "dispatcher.h"
#include "protocol.h"
#include "serialize.h"

namespace djq {

void dispatch_pending_jobs(JobQueue& queue, WorkerPool& workers, WriteAheadLog& wal) {
    while (workers.has_idle()) {
        auto job = queue.try_dispatch();
        if (!job) break; // nothing left pending
        wal.log_dispatched(job->job_id);

        uint64_t worker_id = workers.next();
        workers.assign_job(worker_id, job->job_id);

        Message msg{MessageType::JOB_DISPATCH, encode_job_dispatch(*job)};
        if (!send_message(workers.fd_of(worker_id), msg)) {
            // Worker's socket is already dead. Logging this immediately
            // after logging the dispatch is fine: replay will just see
            // DISPATCHED followed by FAILED_OR_RETRY for the same id, in
            // order, and reproduce exactly this outcome.
            queue.mark_failed_or_retry(job->job_id);
            wal.log_failed_or_retry(job->job_id);
            // Must fully remove, not just mark_idle: a dead connection stays
            // dead, so leaving it "idle" would just make has_idle() pick it
            // again next loop iteration, fail again, forever — this worker
            // never gets evicted. Idempotent if its own connection-handler
            // thread later also detects the disconnect and calls remove().
            workers.remove(worker_id);
        }
    }
}

} // namespace djq
