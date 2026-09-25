#include "job_queue.h"

namespace djq {

void JobQueue::enqueue(const Job& job) {
    order_.push(QueueEntry{job.priority, next_seq_++, job.job_id});
}

uint64_t JobQueue::submit(std::string payload, int priority) {
    Job job;
    job.job_id = next_id_++;
    job.payload = std::move(payload);
    job.priority = priority;
    job.status = JobStatus::PENDING;

    uint64_t id = job.job_id;
    jobs_.emplace(id, std::move(job));
    enqueue(jobs_.at(id));
    return id;
}

std::optional<Job> JobQueue::try_dispatch() {
    // Entries can go stale if a job somehow left PENDING without being
    // popped (shouldn't happen given current call sites, but skip
    // defensively rather than hand out a job in the wrong state).
    while (!order_.empty()) {
        QueueEntry top = order_.top();
        order_.pop();

        auto it = jobs_.find(top.job_id);
        if (it == jobs_.end() || it->second.status != JobStatus::PENDING)
            continue;

        it->second.status = JobStatus::DISPATCHED;
        return it->second;
    }
    return std::nullopt;
}

Job* JobQueue::find(uint64_t job_id) {
    auto it = jobs_.find(job_id);
    return it == jobs_.end() ? nullptr : &it->second;
}

void JobQueue::mark_succeeded(uint64_t job_id) {
    if (Job* job = find(job_id)) {
        job->status = JobStatus::SUCCEEDED;
    }
}

void JobQueue::mark_failed_or_retry(uint64_t job_id) {
    Job* job = find(job_id);
    if (!job) return;

    job->retry_count++;
    if (job->retry_count < MAX_RETRIES) {
        job->status = JobStatus::PENDING;
        enqueue(*job);
    } else {
        job->status = JobStatus::FAILED;
    }
}

void JobQueue::replay_submit(uint64_t job_id, std::string payload, int priority) {
    Job job;
    job.job_id = job_id;
    job.payload = std::move(payload);
    job.priority = priority;
    job.status = JobStatus::PENDING;

    jobs_.emplace(job_id, std::move(job));
    enqueue(jobs_.at(job_id));

    if (job_id >= next_id_) next_id_ = job_id + 1;
}

void JobQueue::replay_dispatched(uint64_t job_id) {
    if (Job* job = find(job_id)) {
        job->status = JobStatus::DISPATCHED;
    }
}

void JobQueue::requeue_orphaned_dispatched_jobs() {
    for (auto& [id, job] : jobs_) {
        if (job.status == JobStatus::DISPATCHED) {
            job.status = JobStatus::PENDING;
            enqueue(job);
        }
    }
}

} // namespace djq
