cmake_minimum_required(VERSION 3.0 FATAL_ERROR)

project(raft-cpp VERSION 1.0)

# enable c++ 11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CRAFT_PUBLIC_INCLUDE_DIR "include/craft")

add_subdirectory(third_party/googletest)
enable_testing()

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
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${PROJECT_SOURCE_DIR}/include --cpp_out=${PROJECT_SOURCE_DIR}/include craft/raft.proto
)

execute_process(
    COMMAND mv ${PROJECT_SOURCE_DIR}/include/craft/raft.pb.cc ${PROJECT_SOURCE_DIR}/src/
)

list(APPEND EXTRA_LIBS "${PROTOBUF_LIBRARY}")
list(APPEND EXTRA_INCLUDES "${PROJECT_SOURCE_DIR}/src" "${Protobuf_INCLUDE_DIRS}")

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${EXTRA_INCLUDES}")

add_library(craft "")
target_sources(craft
    PRIVATE
    "src/common/singleton.h"
    "src/common/fixed_buffer.h"
    "src/common/logger.cc"
    "src/common/logger.h"
    "src/common/status.cc"
    "src/common/util.cc"
    "src/common/util.h"
    "src/raft.pb.cc"
    "src/storage.cc"
    "src/log_unstable.cc"
    "src/log.cc"
    "src/quorum/quorum.h"
    "src/quorum/majority.cc"
    "src/quorum/majority.h"
    "src/quorum/joint.cc"
    "src/quorum/joint.h"

    # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
    $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    "${CRAFT_PUBLIC_INCLUDE_DIR}/raft.pb.h"
    "${CRAFT_PUBLIC_INCLUDE_DIR}/status.h"
    "${CRAFT_PUBLIC_INCLUDE_DIR}/noncopyable.h"
    "${CRAFT_PUBLIC_INCLUDE_DIR}/storage.h"
    "${CRAFT_PUBLIC_INCLUDE_DIR}/log_unstable.h"
    "${CRAFT_PUBLIC_INCLUDE_DIR}/log.h"
)
target_link_libraries(craft "${EXTRA_LIBS}")

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

craft_test("src/storage_test.cc")
craft_test("src/log_unstable_test.cc")
craft_test("src/log_test.cc")
craft_test("src/quorum/majority_test.cc")
craft_test("src/quorum/joint_test.cc")