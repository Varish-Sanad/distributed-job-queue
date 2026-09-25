// Throwaway sanity check for WorkerPool + dispatch_pending_jobs (Section 3).
// Uses socketpair() to stand in for real worker sockets without needing a
// full TCP listener.
#include "job_queue.h"
#include "worker_pool.h"
#include "dispatcher.h"
#include "serialize.h"
#include "protocol.h"
#include "wal.h"

#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>
#include <cassert>
#include <iostream>

int main() {
    djq::JobQueue queue;
    djq::WorkerPool workers;
    const char* wal_path = "/tmp/djq_dispatch_test.wal";
    std::remove(wal_path);
    djq::WriteAheadLog wal(wal_path);

    int w1[2], w2[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, w1);
    socketpair(AF_UNIX, SOCK_STREAM, 0, w2);
    workers.add(1, w1[0]);
    workers.add(2, w2[0]);

    uint64_t job_a = queue.submit("job A", /*priority=*/5);
    uint64_t job_b = queue.submit("job B", /*priority=*/5);

    djq::dispatch_pending_jobs(queue, workers, wal);

    djq::Message recv_a, recv_b;
    assert(djq::receive_message(w1[1], recv_a));
    assert(djq::receive_message(w2[1], recv_b));

    uint64_t id_a, id_b;
    std::string payload_a, payload_b;
    assert(djq::decode_job_dispatch(recv_a.payload, id_a, payload_a));
    assert(djq::decode_job_dispatch(recv_b.payload, id_b, payload_b));

    assert(id_a == job_a && payload_a == "job A");
    assert(id_b == job_b && payload_b == "job B");
    std::cout << "ok: round-robin sent job A to worker 1, job B to worker 2\n";

    assert(queue.find(job_a)->status == djq::JobStatus::DISPATCHED);
    assert(queue.find(job_b)->status == djq::JobStatus::DISPATCHED);
    std::cout << "ok: both jobs marked DISPATCHED\n";

    for (int fd : {w1[0], w1[1], w2[0], w2[1]}) close(fd);
    std::cout << "all dispatch checks passed\n";
    return 0;
}
