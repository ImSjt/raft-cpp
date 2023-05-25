# Raft
## 简介
Raft 是一种分布式一致性算法，旨在管理分布式系统中的复制状态机。它的目的是确保多个服务器之间的数据一致性，尽管存在故障和网络分区。这种算法在现代分布式系统（如 etcd、cockroach 和 tikv）中广泛应用，为维护数据一致性和可靠性提供了坚实的基础。

该库仅包括 raft 算法，将网络层和存储层留给用户自己实现，这使得该库更加灵活和可定制。
## 编译
> - bazel > 3.0
> - c++17

### 编译所有（包括example和所有测试）
```shell
bazel build //...
```
在bazel-bin目录下将生成所有二进制文件
### 运行所有测试
```shell
bazel test //...
```
### 在你的项目中使用

首先在你的**WORKSPACE**添加
```shell
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "raft-cpp",
    branch = "main",
    remote = "https://github.com/ImSjt/raft-cpp.git",
)
```
由于raft-cpp需要使用到protobuf，所以还需要在你的**WORKSPACE**加上下面内容
```shell

# rules_cc defines rules for generating C++ code from Protocol Buffers.
http_archive(
    name = "rules_cc",
    sha256 = "35f2fb4ea0b3e61ad64a369de284e4fbbdcdba71836a5555abb5e194cf119509",
    strip_prefix = "rules_cc-624b5d59dfb45672d4239422fa1e3de1822ee110",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
        "https://github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
    ],
)

# rules_proto defines abstract rules for building Protocol Buffers.
http_archive(
    name = "rules_proto",
    sha256 = "2490dca4f249b8a9a3ab07bd1ba6eca085aaf8e45a734af92aad0c42d9dc7aaf",
    strip_prefix = "rules_proto-218ffa7dfa5408492dc86c01ee637614f8695c45",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/218ffa7dfa5408492dc86c01ee637614f8695c45.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/218ffa7dfa5408492dc86c01ee637614f8695c45.tar.gz",
    ],
)

load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")
rules_cc_dependencies()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()

```
在**BUILD**文件中引用raft-cpp
```shell
package(default_visibility = ["//visibility:public"])
  
cc_binary(
  name = "example",
  srcs = [
    "main.cc",
  ],
  deps = [
    "@raft-cpp//:raft-cpp",
  ],
)
```
开始使用raft-cpp
```c++
#include "rawnode.h"

int main(int argc, char* argv[]) {
  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto storage = std::make_shared<craft::MemoryStorage>(logger);
  craft::Raft::Config cfg {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
    .logger = logger,
  };
  auto node = craft::RawNode::Start(cfg, {craft::Peer{1}});

  return 0;
}
```

## 用法
使用RawNode::Start从头开始建立一个节点，使用RawNode::Restart从某个初始状态启动一个节点。

**启动三节点集群**

```c++
  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto storage = std::make_shared<craft::MemoryStorage>(logger);
  craft::Raft::Config cfg = {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
    .logger = logger,
  };
  // Set peer list to the nodes in the cluster.
  // Note that they need to be started separately as well.
  auto rn = craft::RawNode::Start(cfg, {craft::Peer{1}, craft::Peer{2}, craft::Peer{3}});
```
**添加新节点到集群中**

首先通过向集群中某个节点调用`ProposeConfChange`添加新节点，然后再启动一个空的新节点，如下。
```c++
  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto storage = std::make_shared<craft::MemoryStorage>(logger);
  craft::Raft::Config cfg = {
    .id = 4,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
    .logger = logger,
  };
  // Restart raft without peer information.
  // Peer information should be synchronized from the leader.
  auto rn = craft::RawNode::ReStart(cfg);
```
**使用旧状态启动节点**
```c++
  auto logger = std::make_shared<craft::ConsoleLogger>();
  auto storage = std::make_shared<craft::MemoryStorage>(logger);

  // Recover the in-memory storage from persistent snapshot, state and entries.
  storage->ApplySnapshot(snapshot);
  storage->SetHardState(state);
  storage->Append(entries);

  craft::Raft::Config cfg = {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = storage,
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
    .logger = logger,
  };
  // Restart raft without peer information.
  // Peer information is already included in the storage.
  auto rn = craft::RawNode::ReStart(cfg);
```
**创建节点后，还有一些工作要做**

首先通过RawNode::GetReady()读取并处理最新的更新。

1. 将Entries、HardState、Snapshot持久化，先写入Entries，如果HardState和Snapshot不为空再将它们写入。
2. 将messages发送到指定的节点，如果有MsgSnap类型，在发送完snapshot后要调用RawNode::ReportSnapshot接口。
3. 应用snapshot和committed_entries到状态机中，如果committed_entries中有EntryConfChange类型的entry，那么需要调用RawNode::ApplyConfChange应用。
4. 最后调用RawNode::Advance()表示处理完成，可以接受下一批更新。

**在收到消息时需要调用RawNode::Step处理**
```c++
void RawNode::Step(MsgPtr m);
```
*最后，需要定时调用RawNode::Tick*
```c++
void RawNode::Tick();
```
RawNode::Tick会驱动Raft的心跳机制以及选举机制。

**发送请求需要使用RawNode::Propose处理**

将请求序列化成字符串再发送。
```c++
Status RawNode::Propose(const std::string& data);
```

**处理消息需要调用RawNode::Step处理**

```c++
Status RawNode::Step(MsgPtr m);
```

整个流程类似于下面。
```c++
  while (1) {
    auto items = queue.wait_dequeue(timeout);
    for (auto item : items) {
      if (request) {
        n->Propose(item);
      } else if (message) {
        n->Step(item);
      }
    }

    if (heartbeat_timeout) {
      n->Tick();
    }

    auto ready = n->GetReady();
    saveToStorage(ready->hard_state, ready->entries, ready->snapshot);
    send(ready->messages);
    if (ready->snapshot) {
      processSnapshot(ready->snapshot);
    }

    for (auto entry : ready->committed_entries) {
      process(entry);
      if (entry->type() == raftpb::EntryType::EntryConfChange) {
        raftpb::ConfChange cc;
        cc.ParseFromString(ent->data());
        node->rn->ApplyConfChange(craft::ConfChangeI(std::move(cc)));
      }
    }

    n->Advance();
  }
```
## 感谢
该项目参考了etcd raft的实现，感谢etcd提供了如此优雅的实现。

## 参考
- [Etcd Raft](https://github.com/etcd-io/raft)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [The Raft Site](https://raft.github.io/)
- [The Secret Lives of Data - Raft](https://thesecretlivesofdata.com/raft/)

