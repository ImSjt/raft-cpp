package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common",
    srcs = [
        "logger.cc",
        "status.cc",
        "util.cc",
    ],
    hdrs = [
        "define.h",
        "logger.h",
        "status.h",
        "util.h",
    ],
    deps = [
        "//src/raftpb:raft_cc_proto",
    ],
)

cc_library(
    name = "craft",
    srcs = [
        "log.cc",
        "log_unstable.cc",
        "raft.cc",
        "rawnode.cc",
        "read_only.cc",
        "storage.cc",
    ],
    hdrs = [
        "log.h",
        "log_unstable.h",
        "raft.h",
        "rawnode.h",
        "read_only.h",
        "storage.h",
    ],
    deps = [
        ":common",
        "//src/confchange",
        "//src/quorum",
        "//src/raftpb",
        "//src/tracker",
    ],
)

cc_test(
    name = "log_unstable_test",
    srcs = [
        "log_unstable_test.cc",
    ],
    deps = [
        ":craft",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "log_test",
    srcs = [
        "log_test.cc",
    ],
    deps = [
        ":craft",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "storage_test",
    srcs = [
        "storage_test.cc",
    ],
    deps = [
        ":craft",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "raft_test_util",
    srcs = [
        "raft_test_util.cc",
    ],
    hdrs = [
        "raft_test_util.h",
    ],
    deps = [
        ":craft",
    ],
)

cc_test(
    name = "raft_test",
    srcs = [
        "raft_test.cc",
    ],
    deps = [
        ":raft_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft_paper_test",
    srcs = [
        "raft_paper_test.cc",
    ],
    deps = [
        ":raft_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft_snap_test",
    srcs = [
        "raft_snap_test.cc",
    ],
    deps = [
        ":raft_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raft_flow_control_test",
    srcs = [
        "raft_flow_control_test.cc",
    ],
    deps = [
        ":raft_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "rawnode_test",
    srcs = [
        "rawnode_test.cc",
    ],
    deps = [
        ":raft_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
