#include "protocol.h"
#include <sys/socket.h>
#include <arpa/inet.h>

namespace djq {

namespace {

// TCP is a byte stream, not a message stream (see Section 1, Block 1) — a
// single send()/recv() call is not guaranteed to move all `len` bytes, so
// both directions loop until the full amount has actually moved.
bool write_all(int sockfd, const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sockfd, data + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool read_all(int sockfd, uint8_t* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(sockfd, data + received, len - received, 0);
        if (n <= 0) return false; // 0 = peer closed cleanly, <0 = error
        received += static_cast<size_t>(n);
    }
    return true;
}

} // namespace

bool send_message(int sockfd, const Message& msg) {
    uint32_t payload_len = static_cast<uint32_t>(msg.payload.size());
    uint32_t len_be = htonl(payload_len); // network byte order for the wire
    uint8_t type_byte = static_cast<uint8_t>(msg.type);

    if (!write_all(sockfd, reinterpret_cast<const uint8_t*>(&len_be), sizeof(len_be)))
        return false;
    if (!write_all(sockfd, &type_byte, sizeof(type_byte)))
        return false;
    if (payload_len > 0 && !write_all(sockfd, msg.payload.data(), payload_len))
        return false;
    return true;
}

bool receive_message(int sockfd, Message& out) {
    uint32_t len_be;
    if (!read_all(sockfd, reinterpret_cast<uint8_t*>(&len_be), sizeof(len_be)))
        return false;
    uint32_t payload_len = ntohl(len_be);
    if (payload_len > MAX_MESSAGE_PAYLOAD_SIZE)
        return false; // reject rather than resize() to an untrusted length

    uint8_t type_byte;
    if (!read_all(sockfd, &type_byte, sizeof(type_byte)))
        return false;

    out.type = static_cast<MessageType>(type_byte);
    out.payload.resize(payload_len);
    if (payload_len > 0 && !read_all(sockfd, out.payload.data(), payload_len))
        return false;
    return true;
}

} // namespace djq
