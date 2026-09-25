#pragma once
#include <cstdint>
#include <vector>

namespace djq {

enum class MessageType : uint8_t {
    SUBMIT_JOB   = 1,  // client -> coordinator: new job to enqueue
    JOB_DISPATCH = 2,  // coordinator -> worker: here's a job to run
    JOB_RESULT   = 3,  // worker -> coordinator: job finished (success/fail)
    REGISTER     = 4,  // worker -> coordinator: I'm online, add me to the pool
    HEARTBEAT    = 5,  // worker -> coordinator: I'm still alive
    ACK          = 6,  // generic acknowledgment
    DEREGISTER   = 7,  // worker -> coordinator: shutting down gracefully
};

struct Message {
    MessageType type;
    std::vector<uint8_t> payload;
};

// Hard cap on a single message's payload. The length prefix comes straight
// off the wire from whoever's on the other end — without a bound, a bogus
// (or malicious) length would make receive_message() try to allocate an
// attacker-controlled amount of memory before ever validating anything else.
// 16 MB is generous for anything this project actually sends.
constexpr uint32_t MAX_MESSAGE_PAYLOAD_SIZE = 16 * 1024 * 1024;

// Frames and writes `msg` to sockfd as [4B big-endian length][1B type][payload].
// Returns false on any socket error (caller should treat as a dead connection).
bool send_message(int sockfd, const Message& msg);

// Blocks until one full framed message has been read from sockfd. Returns
// false on disconnect, error, or a length prefix exceeding
// MAX_MESSAGE_PAYLOAD_SIZE (treated the same as a dead connection).
bool receive_message(int sockfd, Message& out);

} // namespace djq
