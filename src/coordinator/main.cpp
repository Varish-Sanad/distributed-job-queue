#include "protocol.h"
#include "job_queue.h"
#include "worker_pool.h"
#include "dispatcher.h"
#include "serialize.h"
#include "wal.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <exception>
#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace {
constexpr int PORT = 8090; // 7000 collides with macOS Control Center/AirPlay
constexpr const char* WAL_PATH = "coordinator.wal";
constexpr int HEARTBEAT_TIMEOUT_SEC = 6;

// std::cout is NOT safe for unsynchronized concurrent writes — multiple
// threads calling operator<< on the same stream race on its internal
// formatting state (confirmed live by ThreadSanitizer during this section's
// testing, not just theoretical). Every log line goes through this instead.
std::mutex g_log_mtx;
void log(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_log_mtx);
    std::cout << msg << "\n" << std::flush;
}

// Everything shared across connection threads, behind ONE mutex. queue,
// workers, and wal are tightly coupled — dispatch_pending_jobs() already
// treats them as a single atomic unit — so one coarse lock protecting all
// three is the correct conservative baseline (separate per-structure locks
// would risk inconsistent lock ordering / deadlock for no real benefit).
// Fine-grained locking is explicitly a stretch goal, not this section's job.
struct SharedState {
    djq::JobQueue queue;
    djq::WorkerPool workers;
    djq::WriteAheadLog wal;
    uint64_t next_worker_id = 1;

    std::mutex mtx;
    // Signaled whenever state changes in a way that might make a dispatch
    // possible (new job, worker freed up, worker reassigned-from). The
    // dispatcher thread is the ONLY thread that actually calls
    // dispatch_pending_jobs() — everyone else just mutates + notifies.
    std::condition_variable dispatch_cv;

    explicit SharedState(const std::string& wal_path) : wal(wal_path) {}
};

// Runs for the coordinator's whole lifetime on its own thread. Sleeps until
// there's actually something dispatchable instead of every mutating thread
// having to remember to call dispatch_pending_jobs() itself.
//
// No graceful shutdown here by design, not oversight — the spec only
// requires graceful shutdown on the WORKER side ("finish current job, then
// deregister"); the coordinator's own lifecycle is external-kill only
// (SIGKILL/SIGTERM), same as every demo in this project. A real graceful
// coordinator stop would also need to track and join every detached
// per-connection thread below, which is a materially bigger change than
// this project's scope calls for.
void run_dispatcher(SharedState& state) {
    std::unique_lock<std::mutex> lock(state.mtx);
    while (true) {
        state.dispatch_cv.wait(lock, [&] {
            return state.workers.has_idle() && state.queue.pending_count() > 0;
        });
        try {
            djq::dispatch_pending_jobs(state.queue, state.workers, state.wal);
        } catch (const std::exception& e) {
            // A WAL write failure (e.g. disk full) throws — letting that
            // escape this thread would call std::terminate() and kill the
            // ENTIRE coordinator over one failed dispatch. Log and keep the
            // dispatcher alive instead; the next notify_one() retries.
            log(std::string("coordinator: dispatch error: ") + e.what());
        }
    }
}

// A job still without a result when a worker is declared dead (timeout or
// disconnect) gets reassigned. Reused for BOTH cases (see JobQueue's
// mark_failed_or_retry comment from Section 2) — unlike WAL-replay-orphan
// recovery (Section 5), this DOES count against the job's retry budget: a
// worker dying mid-connection is a live, observed failure event, not just
// an artifact of how we recover state after our own crash.
void reclaim_worker(uint64_t worker_id, SharedState& state) {
    std::optional<uint64_t> orphaned_job;
    {
        std::lock_guard<std::mutex> lock(state.mtx);
        orphaned_job = state.workers.remove(worker_id);
        if (orphaned_job) {
            state.queue.mark_failed_or_retry(*orphaned_job);
            state.wal.log_failed_or_retry(*orphaned_job);
        }
    }
    if (orphaned_job) {
        log("coordinator: reassigning job " + std::to_string(*orphaned_job) +
            " from dead worker " + std::to_string(worker_id));
        state.dispatch_cv.notify_one();
    }
}

// Owns one worker's connection for as long as it's registered. Runs on its
// own thread now (Section 7) — a slow or frozen worker only blocks this
// thread, not the coordinator's ability to accept everyone else.
void handle_worker_connection(int worker_fd, uint64_t worker_id, SharedState& state) {
    while (true) {
        // select()-with-timeout mirrors the worker's own heartbeat trick
        // (Section 4): silence for too long IS the failure signal, since a
        // hung/partitioned worker never sends a clean disconnect.
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(worker_fd, &readfds);
        timeval tv{HEARTBEAT_TIMEOUT_SEC, 0};

        int ready = select(worker_fd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready == 0) {
            log("coordinator: worker " + std::to_string(worker_id) +
                " missed its heartbeat deadline, declaring it dead");
            reclaim_worker(worker_id, state);
            return;
        }
        if (ready < 0) {
            log("coordinator: select failed for worker " + std::to_string(worker_id));
            reclaim_worker(worker_id, state);
            return;
        }

        djq::Message msg;
        if (!djq::receive_message(worker_fd, msg)) {
            log("coordinator: worker " + std::to_string(worker_id) + " disconnected unexpectedly");
            reclaim_worker(worker_id, state);
            return;
        }
        switch (msg.type) {
            case djq::MessageType::HEARTBEAT:
                log("coordinator: heartbeat from worker " + std::to_string(worker_id));
                break;
            case djq::MessageType::JOB_RESULT: {
                uint64_t job_id;
                bool success;
                if (!djq::decode_job_result(msg.payload, job_id, success)) {
                    log("coordinator: malformed JOB_RESULT from worker " + std::to_string(worker_id));
                    break;
                }

                bool already_terminal;
                djq::JobStatus resulting_status = djq::JobStatus::PENDING;
                {
                    std::lock_guard<std::mutex> lock(state.mtx);
                    // Idempotency check (Section 6, Block 4): a result for a
                    // job already recorded as terminal is a duplicate — e.g.
                    // this worker was declared dead and its job reassigned,
                    // but it actually finished and its late result just
                    // arrived anyway. Applying it again would be wrong.
                    djq::Job* job = state.queue.find(job_id);
                    already_terminal = job && (job->status == djq::JobStatus::SUCCEEDED ||
                                                job->status == djq::JobStatus::FAILED);
                    if (!already_terminal) {
                        if (success) {
                            state.queue.mark_succeeded(job_id);
                            state.wal.log_succeeded(job_id);
                        } else {
                            state.queue.mark_failed_or_retry(job_id);
                            state.wal.log_failed_or_retry(job_id);
                        }
                        // Re-read status rather than assume: a failure only
                        // means "will retry" if it's still PENDING afterward
                        // — mark_failed_or_retry() may have just exhausted
                        // MAX_RETRIES and set it permanently FAILED instead.
                        if (djq::Job* updated = state.queue.find(job_id)) {
                            resulting_status = updated->status;
                        }
                    }
                    state.workers.mark_idle(worker_id);
                }
                if (already_terminal) {
                    log("coordinator: duplicate/late result for job " + std::to_string(job_id) +
                        " ignored (already terminal)");
                } else if (resulting_status == djq::JobStatus::SUCCEEDED) {
                    log("coordinator: job " + std::to_string(job_id) + " SUCCEEDED");
                } else if (resulting_status == djq::JobStatus::FAILED) {
                    log("coordinator: job " + std::to_string(job_id) +
                        " permanently FAILED (exhausted retries)");
                } else {
                    log("coordinator: job " + std::to_string(job_id) + " FAILED (will retry)");
                }
                state.dispatch_cv.notify_one(); // this worker is free again
                break;
            }
            case djq::MessageType::DEREGISTER: {
                log("coordinator: worker " + std::to_string(worker_id) + " deregistered");
                std::lock_guard<std::mutex> lock(state.mtx);
                state.workers.remove(worker_id); // graceful — should have no in-flight job
                return;
            }
            default:
                log("coordinator: unexpected message type " +
                    std::to_string(static_cast<int>(msg.type)) + " from worker");
                break;
        }
    }
}

// One thread per accepted connection — including for the first
// receive_message() call, since even that can block on a slow client. This
// is what actually fixes the "one worker blocks everyone" limitation that's
// been flagged since Section 4.
void handle_connection_impl(int client_fd, SharedState& state) {
    djq::Message msg;
    if (!djq::receive_message(client_fd, msg)) {
        close(client_fd);
        return;
    }

    switch (msg.type) {
        case djq::MessageType::SUBMIT_JOB: {
            int priority;
            std::string payload;
            if (!djq::decode_submit_job(msg.payload, priority, payload)) {
                log("coordinator: malformed SUBMIT_JOB, dropping connection");
                close(client_fd);
                break;
            }

            uint64_t job_id;
            {
                std::lock_guard<std::mutex> lock(state.mtx);
                job_id = state.queue.submit(payload, priority);
                state.wal.log_submitted(job_id, priority, payload);
            }
            log("coordinator: accepted job " + std::to_string(job_id) +
                " (priority=" + std::to_string(priority) + ")");
            state.dispatch_cv.notify_one();

            djq::Message ack{djq::MessageType::ACK, djq::encode_id(job_id)};
            djq::send_message(client_fd, ack);
            close(client_fd);
            break;
        }
        case djq::MessageType::REGISTER: {
            uint64_t worker_id;
            {
                std::lock_guard<std::mutex> lock(state.mtx);
                worker_id = state.next_worker_id++;
                state.workers.add(worker_id, client_fd);
            }
            log("coordinator: worker " + std::to_string(worker_id) + " registered");

            djq::Message ack{djq::MessageType::ACK, djq::encode_id(worker_id)};
            djq::send_message(client_fd, ack);
            state.dispatch_cv.notify_one(); // pick up anything already waiting

            handle_worker_connection(client_fd, worker_id, state);
            close(client_fd);
            break;
        }
        default:
            log("coordinator: message type " + std::to_string(static_cast<int>(msg.type)) +
                " not handled yet");
            close(client_fd);
            break;
    }
}

// Actual std::thread entry point. A WAL write failure (e.g. disk full)
// throws from deep inside handle_connection_impl — an exception escaping a
// thread's entry function calls std::terminate() and kills the WHOLE
// coordinator process over what should be one failed connection. This
// boundary is what actually contains that to just this one client.
void handle_connection(int client_fd, SharedState& state) {
    try {
        handle_connection_impl(client_fd, state);
    } catch (const std::exception& e) {
        log(std::string("coordinator: connection handler error: ") + e.what());
        close(client_fd);
    }
}

} // namespace

int main() {
    // A send() to a socket whose peer already closed its end raises SIGPIPE,
    // which by default terminates the process. dispatcher.cpp and the
    // JOB_RESULT path both already handle a failed send() as data (return
    // false / bool), so the crash-on-write behavior is pure downside here.
    std::signal(SIGPIPE, SIG_IGN);

    SharedState state(WAL_PATH);
    state.wal.replay(state.queue);
    state.queue.requeue_orphaned_dispatched_jobs();
    log("coordinator: recovered " + std::to_string(state.queue.pending_count()) +
        " pending job(s) from " + WAL_PATH);

    // Runs for the coordinator's lifetime; detach() is fine here since there's
    // no graceful coordinator shutdown to join against (unlike the worker's —
    // that was never required by the spec, only the worker's was).
    std::thread(run_dispatcher, std::ref(state)).detach();

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "coordinator: bind failed: " << std::strerror(errno) << "\n";
        return 1;
    }
    if (listen(listen_fd, 16) < 0) {
        std::cerr << "coordinator: listen failed: " << std::strerror(errno) << "\n";
        return 1;
    }
    log("coordinator: listening on port " + std::to_string(PORT));

    // Thread-per-connection: simple and correct, though unbounded under high
    // connection volume — a thread POOL (bounding concurrency, reusing
    // threads) is the natural next step for a real deployment, called out
    // explicitly in the spec as a stretch goal rather than the baseline.
    while (true) {
        int client_fd = accept(listen_fd, nullptr, nullptr);
        if (client_fd < 0) continue;
        std::thread(handle_connection, client_fd, std::ref(state)).detach();
    }
}
