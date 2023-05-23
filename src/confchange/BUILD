package(default_visibility = ["//visibility:public"])

cc_library(
    name = "confchange",
    srcs = [
        "confchange.cc",
        "restore.cc",
    ],
    hdrs = [
        "confchange.h",
        "restore.h",
    ],
    deps = [
        "//src:common",
        "//src/raftpb:raft_cc_proto",
        "//src/tracker",
    ],
)

cc_test(
    name = "cquick_test",
    srcs = [
        "cquick_test.cc",
    ],
    deps = [
        ":confchange",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "confchange_test",
    srcs = [
        "confchange_test.cc",
    ],
    deps = [
        ":confchange",
        "//src/raftpb",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "restore_test",
    srcs = [
        "restore_test.cc",
    ],
    deps = [
        ":confchange",
        "//src/raftpb",
        "@com_google_googletest//:gtest_main",
    ],
)
