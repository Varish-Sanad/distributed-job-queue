cmake_minimum_required(VERSION 3.16)
project(distributed_job_queue CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

include_directories(include)

add_library(djq_common src/common/protocol.cpp src/common/serialize.cpp)

add_library(djq_coordinator
    src/coordinator/job_queue.cpp
    src/coordinator/worker_pool.cpp
    src/coordinator/dispatcher.cpp
    src/coordinator/wal.cpp
)
target_link_libraries(djq_coordinator djq_common)

add_executable(coordinator src/coordinator/main.cpp)
target_link_libraries(coordinator djq_coordinator Threads::Threads)

add_executable(worker src/worker/main.cpp)
target_link_libraries(worker djq_common Threads::Threads)

add_executable(echo_test tests/manual/echo_test.cpp)
target_link_libraries(echo_test djq_common)

add_executable(submit_client tests/manual/submit_client.cpp)
target_link_libraries(submit_client djq_common)

add_executable(duplicate_result_client tests/manual/duplicate_result_client.cpp)
target_link_libraries(duplicate_result_client djq_common)

# --- Unit tests (Section 8, Block 1) ---
enable_testing()

add_executable(job_queue_test tests/manual/job_queue_test.cpp)
target_link_libraries(job_queue_test djq_coordinator)
add_test(NAME job_queue_test COMMAND job_queue_test)

add_executable(dispatch_test tests/manual/dispatch_test.cpp)
target_link_libraries(dispatch_test djq_coordinator)
add_test(NAME dispatch_test COMMAND dispatch_test)

add_executable(wal_test tests/manual/wal_test.cpp)
target_link_libraries(wal_test djq_coordinator)
add_test(NAME wal_test COMMAND wal_test)
