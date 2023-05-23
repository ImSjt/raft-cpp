package(default_visibility = ["//visibility:public"])

cc_binary(
  name = "single_mem_node",
  srcs = [
    "single_mem_node.cc",
  ],
  deps = [
    "//src:craft",
    "@concurrentqueue//:concurrentqueue",
  ],
)

cc_binary(
  name = "multi_mem_node",
  srcs = [
    "multi_mem_node.cc",
  ],
  deps = [
    "//src:craft",
    "@concurrentqueue//:concurrentqueue",
  ],
)
