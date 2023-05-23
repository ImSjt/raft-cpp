package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

cc_proto_library(
    name = "raft_cc_proto",
    deps = [":raft_proto"],
)

proto_library(
    name = "raft_proto",
    srcs = ["raft.proto"],
)

cc_library(
    name = "raftpb",
    srcs = [
        "confchange.cc",
        "confstate.cc",
    ],
    hdrs = [
        "confchange.h",
        "confstate.h",
    ],
    deps = [
        ":raft_cc_proto",
        "//src:common",
    ],
)

cc_test(
    name = "confchange_test",
    srcs = [
        "confchange_test.cc",
    ],
    deps = [
        ":raftpb",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "confstate_test",
    srcs = [
        "confstate_test.cc",
    ],
    deps = [
        ":raftpb",
        "@com_google_googletest//:gtest_main",
    ],
)
