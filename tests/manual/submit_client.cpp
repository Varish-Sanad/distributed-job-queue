// Throwaway CLI to hand-test SUBMIT_JOB against a running coordinator.
#include "protocol.h"
#include "serialize.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <priority> <payload text>\n";
        return 1;
    }
    int priority = std::stoi(argv[1]);
    std::string payload = argv[2];

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8090);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "submit_client: connect failed (is the coordinator running?)\n";
        return 1;
    }

    djq::Message msg{djq::MessageType::SUBMIT_JOB, djq::encode_submit_job(priority, payload)};
    djq::send_message(sockfd, msg);

    djq::Message reply;
    uint64_t job_id;
    if (djq::receive_message(sockfd, reply) && reply.type == djq::MessageType::ACK &&
        djq::decode_id(reply.payload, job_id)) {
        std::cout << "submit_client: job accepted, id=" << job_id << "\n";
    } else {
        std::cerr << "submit_client: did not get a valid ACK\n";
    }

    close(sockfd);
    return 0;
}
