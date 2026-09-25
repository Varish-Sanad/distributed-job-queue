#include "wal.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

namespace djq {

namespace {

void put_u32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

uint32_t get_u32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

void put_u64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8)
        buf.push_back(static_cast<uint8_t>((v >> shift) & 0xFF));
}

uint64_t get_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

bool write_all(int fd, const uint8_t* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, data + written, len - written);
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

} // namespace

WriteAheadLog::WriteAheadLog(const std::string& path) {
    // O_APPEND: every write() atomically lands at the current end of file.
    fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("failed to open WAL file: " + path);
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (fd_ >= 0) close(fd_);
}

void WriteAheadLog::append(WalRecordType type, const std::vector<uint8_t>& payload) {
    // Record layout: [4B payload length][1B type][payload]
    std::vector<uint8_t> record;
    put_u32(record, static_cast<uint32_t>(payload.size()));
    record.push_back(static_cast<uint8_t>(type));
    record.insert(record.end(), payload.begin(), payload.end());

    if (!write_all(fd_, record.data(), record.size())) {
        throw std::runtime_error("WAL write failed");
    }
    // write() only guarantees the kernel has the bytes, not that they've
    // reached the physical disk. fsync forces that — this is the actual
    // durability guarantee a WAL exists to provide, not just "wrote some bytes."
    fsync(fd_);
}

void WriteAheadLog::log_submitted(uint64_t job_id, int priority, const std::string& payload) {
    std::vector<uint8_t> buf;
    put_u64(buf, job_id);
    put_u32(buf, static_cast<uint32_t>(priority));
    buf.insert(buf.end(), payload.begin(), payload.end());
    append(WalRecordType::SUBMITTED, buf);
}

void WriteAheadLog::log_dispatched(uint64_t job_id) {
    std::vector<uint8_t> buf;
    put_u64(buf, job_id);
    append(WalRecordType::DISPATCHED, buf);
}

void WriteAheadLog::log_succeeded(uint64_t job_id) {
    std::vector<uint8_t> buf;
    put_u64(buf, job_id);
    append(WalRecordType::SUCCEEDED, buf);
}

void WriteAheadLog::log_failed_or_retry(uint64_t job_id) {
    std::vector<uint8_t> buf;
    put_u64(buf, job_id);
    append(WalRecordType::FAILED_OR_RETRY, buf);
}

void WriteAheadLog::replay(JobQueue& queue) {
    // pread() at an explicit offset instead of read(), so replay doesn't
    // disturb fd_'s shared file position (which O_APPEND writes rely on).
    off_t offset = 0;
    while (true) {
        uint8_t header[5];
        ssize_t n = pread(fd_, header, sizeof(header), offset);
        if (n == 0) break;                                    // clean EOF
        if (n < static_cast<ssize_t>(sizeof(header))) break;   // truncated header, stop

        uint32_t payload_len = get_u32(header);
        auto type = static_cast<WalRecordType>(header[4]);

        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            ssize_t got = pread(fd_, payload.data(), payload_len, offset + sizeof(header));
            if (got != static_cast<ssize_t>(payload_len)) break; // truncated payload, stop
        }
        offset += static_cast<off_t>(sizeof(header) + payload_len);

        uint64_t job_id = get_u64(payload.data());
        switch (type) {
            case WalRecordType::SUBMITTED: {
                int priority = static_cast<int>(get_u32(payload.data() + 8));
                std::string job_payload(payload.begin() + 12, payload.end());
                queue.replay_submit(job_id, job_payload, priority);
                break;
            }
            case WalRecordType::DISPATCHED:
                queue.replay_dispatched(job_id);
                break;
            case WalRecordType::SUCCEEDED:
                queue.mark_succeeded(job_id);
                break;
            case WalRecordType::FAILED_OR_RETRY:
                queue.mark_failed_or_retry(job_id);
                break;
        }
    }
}

} // namespace djq
