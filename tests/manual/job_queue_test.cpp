// Throwaway sanity check for JobQueue (Section 2). Real automated tests
// come in Section 8 — this just proves the logic isn't broken before we
// build on top of it.
#include "job_queue.h"
#include <cassert>
#include <iostream>

int main() {
    djq::JobQueue q;

    uint64_t low = q.submit("low priority job", 1);
    uint64_t high = q.submit("high priority job", 10);
    uint64_t mid = q.submit("mid priority job", 5);

    // Higher priority should come out first regardless of submit order.
    auto first = q.try_dispatch();
    assert(first.has_value() && first->job_id == high);
    std::cout << "ok: high priority dispatched first\n";

    auto second = q.try_dispatch();
    assert(second.has_value() && second->job_id == mid);
    std::cout << "ok: mid priority dispatched second\n";

    auto third = q.try_dispatch();
    assert(third.has_value() && third->job_id == low);
    std::cout << "ok: low priority dispatched third\n";

    assert(!q.try_dispatch().has_value());
    std::cout << "ok: queue empty after all dispatched\n";

    // Retry path: fail the same job MAX_RETRIES times, expect it to land on FAILED.
    for (int i = 0; i < djq::JobQueue::MAX_RETRIES; i++) {
        q.mark_failed_or_retry(low);
    }
    assert(q.find(low)->status == djq::JobStatus::FAILED);
    std::cout << "ok: job marked FAILED after exhausting retries\n";

    std::cout << "all job_queue checks passed\n";
    return 0;
}
