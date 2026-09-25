#include "protocol.h"
#include "serialize.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <iostream>
#include <unordered_map>

namespace {
constexpr int PORT = 8090;
constexpr int HEARTBEAT_INTERVAL_SEC = 2;

std::atomic<bool> g_shutdown_requested{false};

void handle_sigint(int) {
    g_shutdown_requested = true;
}

// std::cout is NOT safe for unsynchronized concurrent writes (confirmed live
// by ThreadSanitizer, same class of bug fixed in coordinator/main.cpp during
// Section 7 — missed here initially because this file only gained a second
// thread in Section 8, after that fix already landed). Every log line goes
// through this instead of a bare std::cout <<.
std::mutex g_log_mtx;
void log(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_log_mtx);
    std::cout << msg << "\n" << std::flush;
}

// Runs the job's payload as a real shell command (popen -> /bin/sh -c) and
// reports success as a clean (WIFEXITED) zero exit status. stdout is drained
// so the child never blocks on a full pipe even though we don't use it;
// stderr is inherited (goes straight to this worker's own stderr).
//
// Trust note: the payload IS the command, by design — same trust model as
// any job-queue system that executes submitted work (Celery, cron, etc.).
// No sandboxing here; out of scope, not requested.
bool execute_job(const std::string& payload) {
    log("worker: executing job payload=\"" + payload + "\"");

    FILE* pipe = popen(payload.c_str(), "r");
    if (!pipe) {
        log("worker: popen failed for job");
        return false;
    }
    char buf[256];
    while (fgets(buf, sizeof(buf), pipe) != nullptr) {
        // draining output; not captured/reported further — out of scope
    }
    int status = pclose(pipe);
    bool success = WIFEXITED(status) && WEXITSTATUS(status) == 0;

    log("worker: command exited (status=" + std::to_string(status) + ")");
    return success;
}

// Idempotency guard (Section 6, Block 4) plus in-flight tracking, all behind
// one mutex — same reasoning as the coordinator's SharedState: a couple of
// small, tightly-coupled pieces of state, one coarse lock is simpler and
// correct. job_id doubles as its own idempotency key: processed_jobs only
// protects against THIS worker being asked to redo a job it already ran
// (e.g. a duplicate dispatch after a dropped ACK) — it can't stop a
// DIFFERENT worker from re-running the same job_id after this one was
// wrongly declared dead. That broader guarantee has to come from the
// coordinator (see main.cpp) or, in a real system, from whatever downstream
// system the job's side effect actually hits.
struct WorkerState {
    std::mutex mtx;
    std::unordered_map<uint64_t, bool> processed_jobs;
    bool job_in_flight = false;
};

// Serializes every send_message() call on sockfd. Job execution now runs on
// its own thread (see run_job) so it no longer blocks the heartbeat loop —
// but that means two different threads can now want to write to the same
// socket (a heartbeat vs. a job result), and interleaved writes from two
// threads on one connection would corrupt the message framing.
std::mutex g_send_mtx;

bool send_locked(int sockfd, const djq::Message& msg) {
    std::lock_guard<std::mutex> lock(g_send_mtx);
    return djq::send_message(sockfd, msg);
}

// Runs on its own thread per dispatched job. This is the actual fix for the
// limitation flagged back in Section 6/7: execute_job() used to run inline
// on the same thread that was select()-ing for heartbeats, so any job
// longer than the coordinator's heartbeat timeout got this worker wrongly
// declared dead. Now the main loop keeps heartbeating the whole time a job
// runs, regardless of how long it takes.
void run_job(int sockfd, uint64_t job_id, std::string payload, WorkerState& state) {
    bool success = execute_job(payload);

    {
        std::lock_guard<std::mutex> lock(state.mtx);
        state.processed_jobs[job_id] = success;
        state.job_in_flight = false;
    }

    send_locked(sockfd, djq::Message{djq::MessageType::JOB_RESULT,
                                      djq::encode_job_result(job_id, success)});
    log("worker: reported job " + std::to_string(job_id) + (success ? " SUCCEEDED" : " FAILED"));
}

} // namespace

int main() {
    std::signal(SIGINT, handle_sigint);
    // Same reasoning as coordinator/main.cpp: don't let a write to an
    // already-closed coordinator connection kill this process via SIGPIPE.
    std::signal(SIGPIPE, SIG_IGN);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "worker: connect failed (is the coordinator running?)\n";
        return 1;
    }

    djq::send_message(sockfd, djq::Message{djq::MessageType::REGISTER, {}});
    djq::Message reply;
    uint64_t worker_id;
    if (!djq::receive_message(sockfd, reply) || reply.type != djq::MessageType::ACK ||
        !djq::decode_id(reply.payload, worker_id)) {
        std::cerr << "worker: registration failed\n";
        return 1;
    }
    log("worker: registered as worker " + std::to_string(worker_id));

    WorkerState worker_state;
    std::thread job_thread; // tracks the currently in-flight job's thread, if any

    // select() with a timeout gives us "listen for a job, but also do
    // something periodically if idle" on one thread and one socket for I/O
    // — job EXECUTION itself is now offloaded to run_job's own thread (see
    // above), so this loop is never blocked by how long a job takes.
    while (!g_shutdown_requested) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sockfd, &readfds);
        timeval tv{HEARTBEAT_INTERVAL_SEC, 0};

        int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready < 0) {
            if (errno == EINTR) continue; // interrupted by our own SIGINT handler
            std::cerr << "worker: select failed\n";
            break;
        }
        if (ready == 0) {
            send_locked(sockfd, djq::Message{djq::MessageType::HEARTBEAT, {}});
            log("worker: sent heartbeat");
            continue;
        }

        djq::Message msg;
        if (!djq::receive_message(sockfd, msg)) {
            log("worker: coordinator disconnected");
            break;
        }
        if (msg.type == djq::MessageType::JOB_DISPATCH) {
            uint64_t job_id;
            std::string payload;
            if (!djq::decode_job_dispatch(msg.payload, job_id, payload)) {
                std::cerr << "worker: malformed JOB_DISPATCH, ignoring\n";
                continue;
            }

            bool have_cached = false, cached_success = false, already_running = false;
            {
                std::lock_guard<std::mutex> lock(worker_state.mtx);
                auto it = worker_state.processed_jobs.find(job_id);
                if (it != worker_state.processed_jobs.end()) {
                    have_cached = true;
                    cached_success = it->second;
                } else if (worker_state.job_in_flight) {
                    already_running = true; // shouldn't happen given the coordinator's
                                             // busy-tracking, but don't overlap execution if it does
                } else {
                    worker_state.job_in_flight = true;
                }
            }

            if (have_cached) {
                log("worker: job " + std::to_string(job_id) +
                    " already processed (idempotency key hit), skipping re-execution");
                send_locked(sockfd, djq::Message{djq::MessageType::JOB_RESULT,
                                                  djq::encode_job_result(job_id, cached_success)});
            } else if (already_running) {
                std::cerr << "worker: received a new job while already executing one — ignoring\n";
            } else {
                // Reclaim the previous job's thread handle before reassigning —
                // job_in_flight being false only means the WORK is done, not
                // that this std::thread object was ever join()ed; reassigning
                // a still-joinable std::thread calls std::terminate().
                if (job_thread.joinable()) job_thread.join();
                job_thread = std::thread(run_job, sockfd, job_id, payload, std::ref(worker_state));
            }
        }
    }

    // Graceful shutdown means finishing the CURRENT job before deregistering
    // — now that execution is async, that has to be an explicit join(),
    // not just "the loop naturally didn't move on yet" like before.
    if (job_thread.joinable()) {
        log("worker: waiting for in-flight job to finish before shutting down");
        job_thread.join();
    }

    log("worker: shutting down, deregistering");
    send_locked(sockfd, djq::Message{djq::MessageType::DEREGISTER, {}});
    close(sockfd);
    return 0;
}
