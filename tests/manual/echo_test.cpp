// Throwaway sanity check for the framing protocol (Section 1). Not part of
// the real test suite (that's Section 8) — just proves send_message()/
// receive_message() actually round-trip over a real TCP connection.
#include "protocol.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <string>

namespace {

constexpr int PORT = 9999;

void run_server() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(listen_fd, 1);
    std::cout << "server: listening on port " << PORT << "\n";

    int client_fd = accept(listen_fd, nullptr, nullptr);
    std::cout << "server: client connected\n";

    djq::Message msg;
    if (!djq::receive_message(client_fd, msg)) {
        std::cerr << "server: failed to receive message\n";
        return;
    }
    std::string text(msg.payload.begin(), msg.payload.end());
    std::cout << "server: received type=" << static_cast<int>(msg.type)
              << " payload=\"" << text << "\"\n";

    djq::Message ack{djq::MessageType::ACK, {}};
    djq::send_message(client_fd, ack);
    std::cout << "server: sent ACK\n";

    close(client_fd);
    close(listen_fd);
}

void run_client() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "client: connect failed\n";
        return;
    }
    std::cout << "client: connected\n";

    std::string text = "hello from worker";
    djq::Message msg{djq::MessageType::HEARTBEAT,
                      std::vector<uint8_t>(text.begin(), text.end())};
    djq::send_message(sockfd, msg);
    std::cout << "client: sent HEARTBEAT\n";

    djq::Message reply;
    if (djq::receive_message(sockfd, reply)) {
        std::cout << "client: received type=" << static_cast<int>(reply.type) << "\n";
    }
    close(sockfd);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " [server|client]\n";
        return 1;
    }
    std::string mode = argv[1];
    if (mode == "server") run_server();
    else if (mode == "client") run_client();
    else { std::cerr << "unknown mode: " << mode << "\n"; return 1; }
    return 0;
}
