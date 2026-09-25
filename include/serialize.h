#pragma once
#include "job.h"
#include <cstdint>
#include <string>
#include <vector>

// Payload encodings for each MessageType's on-wire body. Multi-byte fields
// use manual big-endian packing (not htonl/htons) so this doesn't depend on
// platform-specific 64-bit byte-order helpers.
//
// Every decode_* function returns bool and rejects a buffer too short to
// hold its fixed-size fields, rather than indexing past the end of it. This
// matters because `bytes` is attacker/bug-controlled network input — a
// malformed or truncated message must fail cleanly, not read out of bounds.
namespace djq {

// SUBMIT_JOB payload: [4B priority][remaining bytes = payload text]
std::vector<uint8_t> encode_submit_job(int priority, const std::string& payload);
bool decode_submit_job(const std::vector<uint8_t>& bytes, int& priority, std::string& payload);

// JOB_DISPATCH payload: [8B job_id][remaining bytes = payload text]
std::vector<uint8_t> encode_job_dispatch(const Job& job);
bool decode_job_dispatch(const std::vector<uint8_t>& bytes, uint64_t& job_id, std::string& payload);

// JOB_RESULT payload: [8B job_id][1B success flag]
std::vector<uint8_t> encode_job_result(uint64_t job_id, bool success);
bool decode_job_result(const std::vector<uint8_t>& bytes, uint64_t& job_id, bool& success);

// Generic 8-byte id, used for ACK replies carrying an assigned job_id or
// worker_id — the two are the same wire shape, just different meanings.
std::vector<uint8_t> encode_id(uint64_t id);
bool decode_id(const std::vector<uint8_t>& bytes, uint64_t& out);

} // namespace djq
