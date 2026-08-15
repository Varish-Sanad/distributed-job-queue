# Distributed Job Queue

A distributed job queue system in C++ that coordinates job execution across multiple worker nodes over a custom TCP protocol — no Redis, RabbitMQ, or ZeroMQ. A coordinator node maintains a durable priority queue (write-ahead log for crash recovery) and dispatches jobs to registered workers, which heartbeat their liveness and report results back. Failed workers are detected via missed heartbeats and their in-flight jobs are reassigned, with idempotency keys guaranteeing at-least-once delivery doesn't double-execute side effects.

## Status

Not started.

## Roadmap

- [ ] Coordinator: TCP job submission (binary/text protocol), in-memory priority queue (job ID, payload, priority, retry count, status)
- [ ] Coordinator: write-ahead log for queue persistence + crash recovery via WAL replay
- [ ] Coordinator: job dispatch via round-robin scheduling
- [ ] Worker: registration + periodic heartbeats to coordinator
- [ ] Worker: job execution (start with sleep+result stub, extend to shell command/computation)
- [ ] Worker: success/failure reporting, graceful shutdown (finish current job, deregister)
- [ ] Fault tolerance: missed-heartbeat detection, reassignment of in-flight jobs from failed workers
- [ ] Fault tolerance: at-least-once delivery with idempotency keys
- [ ] Concurrency: multi-threaded coordinator (accept/dispatch/heartbeat threads or pool), mutex/condvar-guarded shared queue state
- [ ] Scheduling: extend round-robin to least-loaded dispatch
- [ ] Tests: unit tests (queue ops, WAL replay, retry logic), integration test (worker crash mid-job → reassignment), load test (N workers, M jobs, exactly-once completion under injected failures)
- [ ] Stretch: pick 1-2 of — simplified Raft/bully leader election across multiple coordinators, job dependency DAGs, backpressure/rate limiting, metrics endpoint (jobs/sec, queue depth, worker health)

## Tech Stack

C++17, POSIX sockets (or Boost.Asio), CMake

## Dependencies

None — standalone systems project. Not part of the trading-stack chain ([options-pricing-engine](https://github.com/Varish-Sanad/options-pricing-engine) → [backtesting-framework](https://github.com/Varish-Sanad/backtesting-framework) → [statistical-arbitrage-research](https://github.com/Varish-Sanad/statistical-arbitrage-research) → [financial-sentiment-model](https://github.com/Varish-Sanad/financial-sentiment-model) → [flight-dynamics-simulator](https://github.com/Varish-Sanad/flight-dynamics-simulator) → [limit-order-book](https://github.com/Varish-Sanad/limit-order-book) → [compiler-interpreter](https://github.com/Varish-Sanad/compiler-interpreter)); built for distributed-systems/backend-infra signal instead.
