package(default_visibility = ["//visibility:public"])

cc_library(
    name = "tracker",
    srcs = [
        "inflights.cc",
        "progress.cc",
        "state.cc",
        "tracker.cc",
    ],
    hdrs = [
        "inflights.h",
        "progress.h",
        "state.h",
        "tracker.h",
    ],
    deps = [
        "//src:common",
        "//src/quorum",
        "//src/raftpb:raft_cc_proto",
    ],
)

cc_test(
    name = "inflights_test",
    srcs = [
        "inflights_test.cc",
    ],
    deps = [
        ":tracker",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "progress_test",
    srcs = [
        "progress_test.cc",
    ],
    deps = [
        ":tracker",
        "@com_google_googletest//:gtest_main",
    ],
)
