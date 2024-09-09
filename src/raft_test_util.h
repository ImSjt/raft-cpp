// Copyright 2023 juntaosu
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <random>

#include "src/raft.h"
#include "src/raftpb/confchange.h"

class Message {
 public:
  Message() { m = std::make_shared<raftpb::Message>(); }

  Message& Type(raftpb::MessageType type) {
    m->set_type(type);
    return *this;
  }

  Message& From(uint64_t from) {
    m->set_from(from);
    return *this;
  }

  Message& To(uint64_t to) {
    m->set_to(to);
    return *this;
  }

  Message& Term(uint64_t term) {
    m->set_term(term);
    return *this;
  }

  Message& LogTerm(uint64_t log_term) {
    m->set_logterm(log_term);
    return *this;
  }

  Message& Index(uint64_t index) {
    m->set_index(index);
    return *this;
  }

  Message& Entries(craft::EntryPtrs ents) {
    for (auto ent : ents) {
      m->add_entries()->CopyFrom(*ent);
    }
    return *this;
  }

  Message& Commit(uint64_t commit) {
    m->set_commit(commit);
    return *this;
  }

  Message& Snapshot(craft::SnapshotPtr snapshot) {
    m->mutable_snapshot()->CopyFrom(*snapshot);
    return *this;
  }

  Message& Reject(bool reject) {
    m->set_reject(reject);
    return *this;
  }

  Message& RejectHint(uint64_t reject_hint) {
    m->set_rejecthint(reject_hint);
    return *this;
  }

  Message& Context(std::string context) {
    m->set_context(context);
    return *this;
  }

  craft::MsgPtr operator()() { return get(); }

  craft::MsgPtr get() { return m; }

 private:
  craft::MsgPtr m;
};

class Entry {
 public:
  Entry() { ent = std::make_shared<raftpb::Entry>(); }

  Entry& Term(uint64_t term) {
    ent->set_term(term);
    return *this;
  }

  Entry& Index(uint64_t index) {
    ent->set_index(index);
    return *this;
  }

  Entry& Type(raftpb::EntryType type) {
    ent->set_type(type);
    return *this;
  }

  Entry& Data(std::string data) {
    ent->set_data(data);
    return *this;
  }

  craft::EntryPtr operator()() { return get(); }

  craft::EntryPtr get() { return ent; }

 private:
  craft::EntryPtr ent;
};

class ConfState {
 public:
  ConfState() { confstate = std::make_shared<raftpb::ConfState>(); }

  ConfState& Voters(std::vector<uint64_t> voters) {
    for (auto voter : voters) {
      confstate->add_voters(voter);
    }
    return *this;
  }

  ConfState& Learners(std::vector<uint64_t> learners) {
    for (auto learner : learners) {
      confstate->add_learners(learner);
    }
    return *this;
  }

  ConfState& VotersOutgoing(std::vector<uint64_t> voters) {
    for (auto voter : voters) {
      confstate->add_voters_outgoing(voter);
    }
    return *this;
  }

  ConfState& LearnersNext(std::vector<uint64_t> learners) {
    for (auto learner : learners) {
      confstate->add_learners_next(learner);
    }
    return *this;
  }

  ConfState& AutoLeave(bool auto_leave) {
    confstate->set_auto_leave(auto_leave);
    return *this;
  }

  std::shared_ptr<raftpb::ConfState> operator()() { return confstate; }

 private:
  std::shared_ptr<raftpb::ConfState> confstate;
};

class ConfChange {
 public:
  ConfChange() { confchange = std::make_shared<raftpb::ConfChangeV2>(); }

  ConfChange& AddConf(raftpb::ConfChangeType type, uint64_t id) {
    auto cs = confchange->add_changes();
    cs->set_type(type);
    cs->set_node_id(id);
    return *this;
  }

  ConfChange& Transition(raftpb::ConfChangeTransition t) {
    confchange->set_transition(t);
    return *this;
  }

  std::shared_ptr<raftpb::ConfChangeV2> operator()() { return confchange; }

 private:
  std::shared_ptr<raftpb::ConfChangeV2> confchange;
};

#define NEW_MSG() (Message())
#define NEW_ENT() (Entry())
#define NEW_CONF_STATE() (ConfState())
#define NEW_CONF_CHANGE() (ConfChange())

class StateMachince {
 public:
  virtual craft::Status Step(craft::MsgPtr m) = 0;
  virtual craft::MsgPtrs ReadMessages() = 0;
};

class Raft : public StateMachince {
 public:
  static std::shared_ptr<Raft> New(std::unique_ptr<craft::Raft>&& raft) {
    return std::make_shared<Raft>(std::move(raft));
  }

  Raft(std::unique_ptr<craft::Raft>&& raft) : raft_(std::move(raft)) {}

  craft::Status Step(craft::MsgPtr m) override { return raft_->Step(m); }

  craft::MsgPtrs ReadMessages() override {
    auto msgs = raft_->Msgs();
    raft_->ClearMsgs();
    return msgs;
  }

  craft::Raft* Get() { return raft_.get(); }

 private:
  std::unique_ptr<craft::Raft> raft_;
};

class BlackHole : public StateMachince {
 public:
  static std::shared_ptr<BlackHole> New() {
    return std::make_shared<BlackHole>();
  }

  craft::Status Step(craft::MsgPtr m) override { return craft::Status::OK(); }

  craft::MsgPtrs ReadMessages() override { return {}; }
};

class NetWork {
 public:
  using Connem = std::pair<uint64_t, uint64_t>;
  using MsgHook = std::function<bool(craft::MsgPtr)>;
  using ConfigFunc = std::function<void(craft::Raft::Config&)>;

  static std::shared_ptr<NetWork> New(
      std::vector<std::shared_ptr<StateMachince>> peers) {
    return NewWithConfig(nullptr, std::move(peers));
  }

  static std::shared_ptr<NetWork> NewWithConfig(
      ConfigFunc cfg, std::vector<std::shared_ptr<StateMachince>> peers) {
    auto network = std::make_shared<NetWork>(std::move(cfg), std::move(peers));
    return network;
  }

  NetWork(ConfigFunc cfg, std::vector<std::shared_ptr<StateMachince>> peers);

  void Send(craft::MsgPtrs msgs);

  void Drop(uint64_t from, uint64_t to, uint32_t perc);

  void Cut(uint64_t one, uint64_t other);

  void Isolate(uint64_t id);

  void Ignore(raftpb::MessageType t);

  void Recover();

  craft::MsgPtrs Filter(craft::MsgPtrs msgs);

  void SetMsgHook(MsgHook&& hook) { msg_hook_ = std::move(hook); }

  std::map<uint64_t, std::shared_ptr<StateMachince>>& Peers() { return peers_; }
  std::map<uint64_t, std::shared_ptr<craft::MemoryStorage>> Storages() {
    return storages_;
  }

 private:
  std::map<uint64_t, std::shared_ptr<StateMachince>> peers_;
  std::map<uint64_t, std::shared_ptr<craft::MemoryStorage>> storages_;
  std::map<Connem, uint32_t> dropm_;
  std::set<raftpb::MessageType> ignorem_;

  // msg_hook_ is called for each message sent. It may inspect the
  // message and return true to send it or false to drop it.
  MsgHook msg_hook_;
};

using testMemoryStorageOptions =
    std::function<void(std::shared_ptr<craft::MemoryStorage>)>;

std::vector<uint64_t> idsBySize(size_t size);

std::shared_ptr<craft::MemoryStorage> newTestMemoryStorage(
    std::vector<testMemoryStorageOptions> opts);

testMemoryStorageOptions withPeers(std::vector<uint64_t> peers);

testMemoryStorageOptions withLearners(std::vector<uint64_t> learners);

craft::Raft::Config newTestConfig(uint64_t id, int64_t election,
                                  int64_t heartbeat,
                                  std::shared_ptr<craft::Storage> storage);

std::shared_ptr<Raft> newTestRaft(uint64_t id, uint64_t election,
                                  uint64_t heartbeat,
                                  std::shared_ptr<craft::Storage> storage);

std::shared_ptr<Raft> newTestRaftWithConfig(craft::Raft::Config& cfg);

std::shared_ptr<Raft> newTestLearnerRaft(
    uint64_t id, uint64_t election, uint64_t hearbeat,
    std::shared_ptr<craft::Storage> storage);

void preVoteConfig(craft::Raft::Config& cfg);

std::shared_ptr<Raft> entsWithConfig(NetWork::ConfigFunc config_func,
                                     std::vector<uint64_t> terms);

std::shared_ptr<Raft> votedWithConfig(NetWork::ConfigFunc config_func,
                                      uint64_t vote, uint64_t term);

raftpb::ConfChangeV2 makeConfChange(uint64_t id, raftpb::ConfChangeType type);

raftpb::ConfChangeV2 makeConfChange(
    std::vector<std::pair<uint64_t, raftpb::ConfChangeType>> ccs);

std::string raftlogString(craft::RaftLog* l);