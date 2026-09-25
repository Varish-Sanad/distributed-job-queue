#pragma once
#include "job_queue.h"
#include "worker_pool.h"
#include "wal.h"

namespace djq {

// Drains PENDING jobs onto available workers, one JOB_DISPATCH per job,
// stopping as soon as either the queue or the worker pool runs out. A job is
// only popped from the queue once we know there's somewhere to send it.
// Every dispatch (and any resulting failure, if the send itself fails) is
// logged to `wal` so a crash mid-dispatch is still recoverable.
void dispatch_pending_jobs(JobQueue& queue, WorkerPool& workers, WriteAheadLog& wal);

} // namespace djq
