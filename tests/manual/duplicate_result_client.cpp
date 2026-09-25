// Live proof for Section 6 Block 4 (coordinator-side idempotency dedup).
// Plays the worker role directly: registers, lets a submitted job get
// dispatched to it, then reports SUCCEEDED for that job_id TWICE on the
// SAME still-open connection. The coordinator should apply the first
// result and log the second as an ignored duplicate.
#include "protocol.h"
#include "serialize.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <iostream>

int main() {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8090);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    // Submit the job FIRST, on its own connection, before we register as a
    // worker. Not strictly required anymore — the coordinator is concurrent
    // since Section 7, so a submit arriving after we'd registered would work
    // fine too — but this ordering keeps the test deterministic (the job is
    // guaranteed PENDING and dispatched to us immediately on REGISTER,
    // rather than racing the dispatcher thread).
    int submit_fd = socket(AF_INET, SOCK_STREAM, 0);
    connect(submit_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    djq::send_message(submit_fd, djq::Message{djq::MessageType::SUBMIT_JOB,
                                               djq::encode_submit_job(1, "dedup demo job")});
    djq::Message submit_ack;
    djq::receive_message(submit_fd, submit_ack);
    close(submit_fd);

    // Now register — since the job is already PENDING and we're the only
    // worker, the coordinator's REGISTER handling dispatches it to us
    // immediately, before it ever starts select()-ing on our connection.
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "duplicate_result_client: connect failed\n";
        return 1;
    }
    djq::send_message(sockfd, djq::Message{djq::MessageType::REGISTER, {}});
    djq::Message reply;
    uint64_t worker_id;
    if (!djq::receive_message(sockfd, reply) || !djq::decode_id(reply.payload, worker_id)) {
        std::cerr << "duplicate_result_client: registration failed\n";
        return 1;
    }
    std::cout << "duplicate_result_client: registered as worker " << worker_id << "\n" << std::flush;

    djq::Message dispatch_msg;
    if (!djq::receive_message(sockfd, dispatch_msg) ||
        dispatch_msg.type != djq::MessageType::JOB_DISPATCH) {
        std::cerr << "duplicate_result_client: did not receive JOB_DISPATCH\n";
        return 1;
    }
    uint64_t job_id;
    std::string payload;
    if (!djq::decode_job_dispatch(dispatch_msg.payload, job_id, payload)) {
        std::cerr << "duplicate_result_client: malformed JOB_DISPATCH\n";
        return 1;
    }
    std::cout << "duplicate_result_client: got job " << job_id
              << ", reporting SUCCEEDED twice\n" << std::flush;

    djq::send_message(sockfd, djq::Message{djq::MessageType::JOB_RESULT,
                                            djq::encode_job_result(job_id, true)});
    // Small pause so the first result is fully processed before the
    // duplicate arrives — keeps the coordinator's log ordering unambiguous.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    djq::send_message(sockfd, djq::Message{djq::MessageType::JOB_RESULT,
                                            djq::encode_job_result(job_id, true)});
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    djq::send_message(sockfd, djq::Message{djq::MessageType::DEREGISTER, {}});
    close(sockfd);
    return 0;
}
