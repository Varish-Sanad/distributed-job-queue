#include "serialize.h"

namespace djq {

namespace {

void put_u32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

uint32_t get_u32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) |
           static_cast<uint32_t>(p[3]);
}

void put_u64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8)
        buf.push_back(static_cast<uint8_t>((v >> shift) & 0xFF));
}

uint64_t get_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = (v << 8) | p[i];
    return v;
}

} // namespace

std::vector<uint8_t> encode_submit_job(int priority, const std::string& payload) {
    std::vector<uint8_t> buf;
    put_u32(buf, static_cast<uint32_t>(priority));
    buf.insert(buf.end(), payload.begin(), payload.end());
    return buf;
}

bool decode_submit_job(const std::vector<uint8_t>& bytes, int& priority, std::string& payload) {
    if (bytes.size() < 4) return false;
    priority = static_cast<int>(get_u32(bytes.data()));
    payload.assign(bytes.begin() + 4, bytes.end());
    return true;
}

std::vector<uint8_t> encode_job_dispatch(const Job& job) {
    std::vector<uint8_t> buf;
    put_u64(buf, job.job_id);
    buf.insert(buf.end(), job.payload.begin(), job.payload.end());
    return buf;
}

bool decode_job_dispatch(const std::vector<uint8_t>& bytes, uint64_t& job_id, std::string& payload) {
    if (bytes.size() < 8) return false;
    job_id = get_u64(bytes.data());
    payload.assign(bytes.begin() + 8, bytes.end());
    return true;
}

std::vector<uint8_t> encode_job_result(uint64_t job_id, bool success) {
    std::vector<uint8_t> buf;
    put_u64(buf, job_id);
    buf.push_back(success ? 1 : 0);
    return buf;
}

bool decode_job_result(const std::vector<uint8_t>& bytes, uint64_t& job_id, bool& success) {
    if (bytes.size() < 9) return false;
    job_id = get_u64(bytes.data());
    success = bytes[8] != 0;
    return true;
}

std::vector<uint8_t> encode_id(uint64_t id) {
    std::vector<uint8_t> buf;
    put_u64(buf, id);
    return buf;
}

bool decode_id(const std::vector<uint8_t>& bytes, uint64_t& out) {
    if (bytes.size() < 8) return false;
    out = get_u64(bytes.data());
    return true;
}

} // namespace djq
