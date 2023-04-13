cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(raft-cpp)
set(PROJECT_VERSION, 1)
set(PROJECT_VERSION_MAJOR, 0)
set(PROJECT_VERSION_MINOR, 0)

# enable c++ 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CRAFT_PUBLIC_INCLUDE_DIR "include/craft")

add_subdirectory(third_party/googletest)
enable_testing()

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(WITH_GTEST OFF CACHE BOOL "" FORCE)
set(WITH_UNWIND OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/glog)
include_directories(${CMAKE_SOURCE_DIR}/src/thirdparty/glog/src)

include(FindProtobuf)
find_package(Protobuf REQUIRED)
get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")

# file(GLOB protobuf_files
#     "src/raft/raftpb/*.proto"
# )

# FOREACH(FILE ${protobuf_files})
#     get_filename_component(FILE_DIR ${FILE} DIRECTORY)
#     get_filename_component(FILE_NAME ${FILE} NAME)

#     set(PROTO_FLAGS "-I${FILE_DIR}")

#     EXECUTE_PROCESS(
#         COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${FILE_DIR} ${FILE_NAME}
#     )
# ENDFOREACH()

execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${PROJECT_SOURCE_DIR}/src/raftpb --cpp_out=${PROJECT_SOURCE_DIR}/src/raftpb raft.proto
)

# execute_process(
#     COMMAND mv ${PROJECT_SOURCE_DIR}/include/craft/raft.pb.cc ${PROJECT_SOURCE_DIR}/src/
# )

list(APPEND EXTRA_LIBS "${PROTOBUF_LIBRARY}")
list(APPEND EXTRA_INCLUDES "${PROJECT_SOURCE_DIR}/src" "${Protobuf_INCLUDE_DIRS}")

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${EXTRA_INCLUDES}")

add_library(craft "")
target_sources(craft
    # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
    PUBLIC
    "src/confchange/confchange.cc"
    "src/confchange/confchange.h"
    "src/confchange/restore.cc"
    "src/confchange/restore.h"
    "src/confchange/restore.h"
    "src/define.h"
    "src/fixed_buffer.h"
    "src/log_unstable.cc"
    "src/log_unstable.h"
    "src/log.cc"
    "src/log.h"
    "src/logger.cc"
    "src/logger.h"
    "src/quorum/joint.cc"
    "src/quorum/joint.h"
    "src/quorum/majority.cc"
    "src/quorum/majority.h"
    "src/quorum/quorum.h"
    "src/raftpb/raft.pb.cc"
    "src/raftpb/raft.pb.h"
    "src/raftpb/confstate.cc"
    "src/raftpb/confstate.h"
    "src/raftpb/confchange.cc"
    "src/raftpb/confchange.h"
    "src/singleton.h"
    "src/status.cc"
    "src/status.h"
    "src/storage.cc"
    "src/storage.h"
    "src/tracker/inflights.cc"
    "src/tracker/inflights.h"
    "src/tracker/progress.cc"
    "src/tracker/progress.h"
    "src/tracker/tracker.cc"
    "src/tracker/tracker.h"
    "src/tracker/state.cc"
    "src/tracker/state.h"
    "src/util.cc"
    "src/util.h"
    "src/read_only.cc"
    "src/read_only.h"
    "src/raft.cc"
    "src/raft.h"
    "src/rawnode.cc"
    "src/rawnode.h"
)
target_link_libraries(craft "${EXTRA_LIBS}")
target_link_libraries (craft glog::glog)

# add_executable(test_bug "src/raft/main.cc")
# target_link_libraries(test_bug craft)

function(craft_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)
    add_executable("${test_target_name}" "")
    target_sources("${test_target_name}"
        PRIVATE
        "${test_file}"
    )

    target_link_libraries("${test_target_name}" gtest gtest_main craft)

    add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")

endfunction(craft_test)

# craft_test("src/storage_test.cc")
# craft_test("src/log_unstable_test.cc")
# craft_test("src/log_test.cc")
craft_test("src/quorum/quick_test.cc")
craft_test("src/quorum/majority_test.cc")
craft_test("src/quorum/joint_test.cc")
craft_test("src/tracker/inflights_test.cc")
craft_test("src/tracker/progress_test.cc")
craft_test("src/raftpb/confstate_test.cc")
craft_test("src/raftpb/confchange_test.cc")
craft_test("src/confchange/cquick_test.cc")
craft_test("src/confchange/restore_test.cc")
craft_test("src/confchange/conf_change_test.cc")