#!/usr/bin/env bash
# Integration test (Section 8, Block 2): freezes a worker mid-connection
# (SIGSTOP, simulating a real hang — not a clean exit) and verifies the
# coordinator detects it via heartbeat timeout, reassigns its job to a
# second worker, and the job completes despite the failure.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD="$ROOT/build"
LOGDIR="$(mktemp -d)"
WORKER1_PID=""

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; echo "--- coordinator.log ---"; cat "$LOGDIR/coordinator.log" 2>/dev/null; cleanup; exit 1; }

cleanup() {
    [ -n "$WORKER1_PID" ] && kill -CONT "$WORKER1_PID" >/dev/null 2>&1
    pkill -f "$BUILD/coordinator" >/dev/null 2>&1
    pkill -f "$BUILD/worker" >/dev/null 2>&1
}
trap cleanup EXIT

echo "=== worker_crash_test: building ==="
mkdir -p "$BUILD"
# Always rebuild (not "if missing") — a stale binary silently testing old
# source would defeat the point of an integration test.
CORE_SRC="$ROOT/src/common/protocol.cpp $ROOT/src/common/serialize.cpp $ROOT/src/coordinator/job_queue.cpp $ROOT/src/coordinator/worker_pool.cpp $ROOT/src/coordinator/dispatcher.cpp $ROOT/src/coordinator/wal.cpp"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/coordinator" $CORE_SRC "$ROOT/src/coordinator/main.cpp" || fail "coordinator build failed"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/worker" "$ROOT/src/common/protocol.cpp" "$ROOT/src/common/serialize.cpp" "$ROOT/src/worker/main.cpp" || fail "worker build failed"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/submit_client" "$ROOT/src/common/protocol.cpp" "$ROOT/src/common/serialize.cpp" "$ROOT/tests/manual/submit_client.cpp" || fail "submit_client build failed"

echo "=== starting coordinator ==="
# Run with cwd = LOGDIR so coordinator.wal lands in this run's isolated temp
# dir, not the project root — avoids relying on cleanup() timing (pkill is
# async; a straggler write between "kill" and "rm coordinator.wal" could
# otherwise recreate stale state a later run would wrongly pick up).
(cd "$LOGDIR" && "$BUILD/coordinator" > "$LOGDIR/coordinator.log" 2>&1) &
sleep 1

echo "=== submitting job before any worker connects ==="
# Payload is now a REAL shell command (Section 8 fix: workers actually
# execute it via popen(), not a sleep-stub) — must be valid, not descriptive
# text. A 2s real delay is deliberate: job execution now runs on its own
# thread (Section 8 fix), so a fast "true" can complete and report back
# before this script's kill -STOP even gets delivered — a genuine race in
# the TEST, not the product. The delay gives freezing a reliable margin.
"$BUILD/submit_client" 1 "sleep 2 && true" || fail "submit_client failed"

echo "=== starting worker1 ==="
"$BUILD/worker" > "$LOGDIR/worker1.log" 2>&1 &
WORKER1_PID=$!

# Freeze worker1 the instant it's confirmed registered, rather than a blind
# sleep — this is robust to exactly when the job's JOB_DISPATCH lands
# relative to the freeze: either way, WorkerPool has already recorded it as
# holding the job by the time anything is sent, so the outcome is the same.
DEADLINE=$((SECONDS + 5))
while [ $SECONDS -lt $DEADLINE ]; do
    grep -q "worker 1 registered" "$LOGDIR/coordinator.log" 2>/dev/null && break
    sleep 0.05
done
grep -q "worker 1 registered" "$LOGDIR/coordinator.log" || fail "worker1 never registered"

kill -STOP "$WORKER1_PID" 2>/dev/null || fail "could not freeze worker1 (pid $WORKER1_PID)"
pass "worker1 frozen right after registering"

echo "=== starting worker2 (healthy) ==="
"$BUILD/worker" > "$LOGDIR/worker2.log" 2>&1 &

echo "=== waiting for heartbeat timeout + reassignment (up to 15s) ==="
DEADLINE=$((SECONDS + 15))
while [ $SECONDS -lt $DEADLINE ]; do
    grep -q "job 1 SUCCEEDED" "$LOGDIR/coordinator.log" 2>/dev/null && break
    sleep 0.5
done

grep -q "missed its heartbeat deadline" "$LOGDIR/coordinator.log" || fail "worker1 was never declared dead"
pass "coordinator detected the frozen worker via heartbeat timeout"

grep -q "reassigning job 1 from dead worker" "$LOGDIR/coordinator.log" || fail "job was never reassigned"
pass "job 1 was reassigned to another worker"

grep -q "job 1 SUCCEEDED" "$LOGDIR/coordinator.log" || fail "job 1 never completed after reassignment"
pass "job 1 completed successfully despite the worker crash"

echo "=== worker_crash_test: ALL CHECKS PASSED ==="
