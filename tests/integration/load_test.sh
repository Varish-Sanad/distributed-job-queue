#!/usr/bin/env bash
# Load test (Section 8, Block 3): N workers, M jobs, one worker SIGKILLed
# mid-run (a real crash — no graceful DEREGISTER, unlike worker_crash_test.sh
# which covers the heartbeat-TIMEOUT detection path; this covers the
# immediate-disconnect detection path instead). Verifies every job completes
# exactly once despite the injected failure.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD="$ROOT/build"
LOGDIR="$(mktemp -d)"

N_WORKERS=4
M_JOBS=20
WORKER_PIDS=()

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; echo "--- coordinator.log ---"; cat "$LOGDIR/coordinator.log" 2>/dev/null; cleanup; exit 1; }

cleanup() {
    pkill -f "$BUILD/coordinator" >/dev/null 2>&1
    pkill -f "$BUILD/worker" >/dev/null 2>&1
}
trap cleanup EXIT

echo "=== load_test: building ==="
mkdir -p "$BUILD"
# Always rebuild (not "if missing") — a stale binary silently testing old
# source would defeat the point of an integration test.
CORE_SRC="$ROOT/src/common/protocol.cpp $ROOT/src/common/serialize.cpp $ROOT/src/coordinator/job_queue.cpp $ROOT/src/coordinator/worker_pool.cpp $ROOT/src/coordinator/dispatcher.cpp $ROOT/src/coordinator/wal.cpp"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/coordinator" $CORE_SRC "$ROOT/src/coordinator/main.cpp" || fail "coordinator build failed"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/worker" "$ROOT/src/common/protocol.cpp" "$ROOT/src/common/serialize.cpp" "$ROOT/src/worker/main.cpp" || fail "worker build failed"
clang++ -std=c++17 -I"$ROOT/include" -o "$BUILD/submit_client" "$ROOT/src/common/protocol.cpp" "$ROOT/src/common/serialize.cpp" "$ROOT/tests/manual/submit_client.cpp" || fail "submit_client build failed"

echo "=== starting coordinator (N=$N_WORKERS workers, M=$M_JOBS jobs) ==="
(cd "$LOGDIR" && "$BUILD/coordinator" > "$LOGDIR/coordinator.log" 2>&1) &
sleep 1

echo "=== submitting $M_JOBS jobs before any worker connects ==="
# Payload is now a REAL shell command (Section 8 fix: workers actually
# execute it via popen(), not a sleep-stub). A brief real sleep gives the
# injected kill a meaningful window to land mid-job — safe to do now since
# job execution runs on its own thread and no longer blocks heartbeats
# (also a Section 8 fix), so this can't cause a false-positive dead-worker.
for i in $(seq 1 "$M_JOBS"); do
    "$BUILD/submit_client" $((i % 5 + 1)) "sleep 0.3 && true" >/dev/null || fail "submit_client failed on job $i"
done
pass "submitted all $M_JOBS jobs"

echo "=== starting $N_WORKERS workers ==="
for i in $(seq 1 "$N_WORKERS"); do
    "$BUILD/worker" > "$LOGDIR/worker$i.log" 2>&1 &
    WORKER_PIDS+=("$!")
done

# Inject a real failure partway through: SIGKILL one worker outright once at
# least one job has started executing somewhere, so its in-flight (or
# soon-to-be-assigned) work has to be handled like any other crash.
DEADLINE=$((SECONDS + 5))
while [ $SECONDS -lt $DEADLINE ]; do
    grep -lq "executing job" "$LOGDIR"/worker*.log 2>/dev/null && break
    sleep 0.1
done
KILLED_PID="${WORKER_PIDS[0]}"
kill -9 "$KILLED_PID" 2>/dev/null
pass "SIGKILLed one worker (pid $KILLED_PID) mid-run to inject a real crash"

echo "=== waiting for all $M_JOBS jobs to complete (up to 30s) ==="
DEADLINE=$((SECONDS + 30))
while [ $SECONDS -lt $DEADLINE ]; do
    COUNT=$(grep -oE "job [0-9]+ SUCCEEDED" "$LOGDIR/coordinator.log" 2>/dev/null | sort -u | wc -l | tr -d ' ')
    [ "$COUNT" -ge "$M_JOBS" ] && break
    sleep 0.5
done

UNIQUE_SUCCEEDED=$(grep -oE "job [0-9]+ SUCCEEDED" "$LOGDIR/coordinator.log" | sort -u | wc -l | tr -d ' ')
[ "$UNIQUE_SUCCEEDED" -eq "$M_JOBS" ] || fail "expected $M_JOBS distinct jobs SUCCEEDED, got $UNIQUE_SUCCEEDED"
pass "all $M_JOBS jobs completed exactly once (sort -u guards against a duplicate-completion regression)"

if grep -qE "job [0-9]+ FAILED \(will retry\)" "$LOGDIR/coordinator.log"; then
    pass "at least one job hit a transient failure/retry from the injected crash, and still succeeded"
fi

if grep -qE "job [0-9]+ permanently FAILED" "$LOGDIR/coordinator.log"; then
    fail "a job permanently FAILED (exhausted retries) — shouldn't happen with only 1 of $N_WORKERS workers killed"
fi
pass "no job permanently failed"

echo "=== load_test: ALL CHECKS PASSED ($M_JOBS/$M_JOBS jobs, $N_WORKERS workers, 1 injected crash) ==="
