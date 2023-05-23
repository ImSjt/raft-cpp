package(default_visibility = ["//visibility:public"])

cc_library(
    name = "quorum",
    srcs = [
        "joint.cc",
        "majority.cc",
    ],
    hdrs = [
        "joint.h",
        "majority.h",
        "quorum.h",
    ],
)

cc_test(
    name = "quick_test",
    srcs = [
        "quick_test.cc",
    ],
    deps = [
        ":quorum",
        "//src:common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "majority_test",
    srcs = [
        "majority_test.cc",
    ],
    deps = [
        ":quorum",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "joint_test",
    srcs = [
        "joint_test.cc",
    ],
    deps = [
        ":quorum",
        "@com_google_googletest//:gtest_main",
    ],
)
