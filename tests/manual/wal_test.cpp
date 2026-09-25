// Throwaway sanity check for WriteAheadLog (Section 5). Simulates a crash
// by just destructing one JobQueue and building a fresh one from the same
// log file — no real process kill needed to test the replay logic itself
// (that's the full end-to-end demo, done separately against real binaries).
#include "wal.h"
#include "job_queue.h"
#include <cassert>
#include <cstdio>
#include <iostream>

int main() {
    const char* path = "/tmp/djq_wal_test.wal";
    std::remove(path); // start clean

    uint64_t job_a, job_b, job_c;
    {
        djq::JobQueue queue;
        djq::WriteAheadLog wal(path);

        job_a = queue.submit("job A", 5);
        wal.log_submitted(job_a, 5, "job A");

        job_b = queue.submit("job B", 10);
        wal.log_submitted(job_b, 10, "job B");

        auto dispatched = queue.try_dispatch(); // should be job_b (higher priority)
        assert(dispatched->job_id == job_b);
        wal.log_dispatched(job_b);

        queue.mark_succeeded(job_b);
        wal.log_succeeded(job_b);

        job_c = queue.submit("job C", 1);
        wal.log_submitted(job_c, 1, "job C");

        auto dispatched2 = queue.try_dispatch(); // should be job_a (only PENDING left w/ higher pri)
        assert(dispatched2->job_id == job_a);
        wal.log_dispatched(job_a);
        // Deliberately do NOT log a result for job_a — simulates a crash
        // while it was still in flight on some worker.
    }
    // "queue" above just went out of scope — that's the crash.

    djq::JobQueue recovered;
    {
        djq::WriteAheadLog wal(path);
        wal.replay(recovered);
    }

    assert(recovered.find(job_b)->status == djq::JobStatus::SUCCEEDED);
    std::cout << "ok: job B replayed as SUCCEEDED\n";

    assert(recovered.find(job_c)->status == djq::JobStatus::PENDING);
    std::cout << "ok: job C replayed as PENDING (never got that far)\n";

    // job_a was left DISPATCHED at "crash" time — replay reproduces exactly
    // that, on purpose. Reconciling it back to PENDING is a separate,
    // explicit policy step (requeue_orphaned_dispatched_jobs), not implied
    // by replay itself.
    assert(recovered.find(job_a)->status == djq::JobStatus::DISPATCHED);
    std::cout << "ok: job A replayed as DISPATCHED (orphaned, not yet reconciled)\n";

    recovered.requeue_orphaned_dispatched_jobs();
    assert(recovered.find(job_a)->status == djq::JobStatus::PENDING);
    std::cout << "ok: orphaned job A reconciled back to PENDING\n";

    // A fresh submission after recovery must not collide with replayed IDs.
    uint64_t job_d = recovered.submit("job D", 1);
    assert(job_d != job_a && job_d != job_b && job_d != job_c);
    std::cout << "ok: post-recovery job IDs don't collide with replayed ones (job_d="
              << job_d << ")\n";

    std::cout << "all WAL checks passed\n";
    return 0;
}
