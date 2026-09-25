#include "worker_pool.h"
#include <algorithm>

namespace djq {

void WorkerPool::add(uint64_t worker_id, int socket_fd) {
    workers_[worker_id] = WorkerInfo{socket_fd, /*busy=*/false, /*current_job=*/std::nullopt};
    order_.push_back(worker_id);
}

std::optional<uint64_t> WorkerPool::remove(uint64_t worker_id) {
    std::optional<uint64_t> orphaned_job;
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        orphaned_job = it->second.current_job;
        workers_.erase(it);
    }

    auto pos = std::find(order_.begin(), order_.end(), worker_id);
    if (pos != order_.end()) {
        // Erasing shifts every later element left by one, so cursor_ needs
        // to shift with it — otherwise it silently skips (or repeats) a
        // worker on the next next() call, quietly breaking round-robin
        // fairness without ever crashing or misdispatching.
        size_t removed_idx = static_cast<size_t>(std::distance(order_.begin(), pos));
        order_.erase(pos);
        if (removed_idx < cursor_) cursor_--;
    }
    if (order_.empty() || cursor_ >= order_.size()) cursor_ = 0;

    return orphaned_job;
}

bool WorkerPool::has_idle() const {
    for (const auto& id : order_) {
        if (!workers_.at(id).busy) return true;
    }
    return false;
}

uint64_t WorkerPool::next() {
    // Scan forward from cursor_ (wrapping) for the next idle worker — plain
    // round-robin without this skip would hand a busy worker a second job.
    for (size_t i = 0; i < order_.size(); i++) {
        size_t idx = (cursor_ + i) % order_.size();
        uint64_t id = order_[idx];
        if (!workers_.at(id).busy) {
            workers_.at(id).busy = true;
            cursor_ = (idx + 1) % order_.size();
            return id;
        }
    }
    return 0; // unreachable if caller checked has_idle() first
}

int WorkerPool::fd_of(uint64_t worker_id) const {
    return workers_.at(worker_id).socket_fd;
}

void WorkerPool::assign_job(uint64_t worker_id, uint64_t job_id) {
    workers_.at(worker_id).current_job = job_id;
}

void WorkerPool::mark_idle(uint64_t worker_id) {
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        it->second.busy = false;
        it->second.current_job.reset();
    }
}

} // namespace djq
