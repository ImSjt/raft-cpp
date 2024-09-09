# Raft
[中文介绍](./README_zh-cn.md)
## Abstract
Raft is a distributed consensus algorithm designed to manage replication of state machines in distributed systems. Its purpose is to ensure data consistency across multiple servers, despite failures and network partitions. It is widely used in modern distributed systems like etcd, cockroach, and tikv, providing a solid foundation for maintaining data consistency and reliability.

The library includes only the raft algorithm, leaving the network and storage layers to be implemented by the user, which makes the library more flexible and customizable.
## Build
> - bazel > 3.0
> - c++17

### Build all
```shell
bazel build //...
```
All binary files will be generated in the bazel-bin directory.
### Build all tests
```shell
bazel test //...
```
### Using in your project
First add the following to your **WORKSPACE**.

```shell
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "raft-cpp",
    branch = "main",
    remote = "https://github.com/ImSjt/raft-cpp.git",
)
```
Since the library depends on protobuf, you also need to add the following to your **WORKSPACE**.

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
Reference the library in **BUILD** file.

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
Start using the raft-cpp.
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

## Usage
Start a node from scratch using RawNode::Start or start a node from some initial state using RawNode::Restart.

**To start a three-node cluster**

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
**Adding a node to the cluster**

First add a new node by calling `Rawnode::ProposeConfChange` to one of the nodes in the cluster, and then start an empty new node as follows.

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
**To restart a node from state**
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
**After creating the node, there is still some work to be done**

First read ready by RawNode::GetReady and process the updates it contains.

1. Persist Entries, HardState, Snapshot, write to Entries first, and then write to HardState and Snapshot if they are not empty.
2. Send the messages to the specified node. If there is a MsgSnap type, call the RawNode::ReportSnapshot after sending the snapshot.
3. Apply snapshot and committed_entries to the state machine, if the committed_entries have EntryConfChange type entries, then you need to call RawNode::ApplyConfChange to apply.
4. The final call to RawNode::Advance() indicates that processing is complete and the next batch of updates can be accepted.

**Call awNode::Tick at regular intervals**
```c++
void RawNode::Tick();
```
RawNode::Tick will drive Raft's heartbeat and election timeout.

**RawNode::Step needs to be called when a message is received to process**
```c++
void RawNode::Step(MsgPtr m);
```

**Send request using RawNode::Propose**

Serialize the request into a string before sending it.
```c++
Status RawNode::Propose(const std::string& data);
```

The whole process is similar to the following.
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
## Acknowledgements
This project references the go implementation of etcd raft, thanks to etcd for providing such an elegant implementation.
## Reference
- [Etcd Raft](https://github.com/etcd-io/raft)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [The Raft Site](https://raft.github.io/)
- [The Secret Lives of Data - Raft](https://thesecretlivesofdata.com/raft/)

