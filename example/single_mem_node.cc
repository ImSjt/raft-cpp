#include "rawnode.h"

int main(int argc, char* argv[]) {
  // storage->GetSnapshot()->mutable_metadata()->mutable_conf_state()->add_voters(1);
  craft::Raft::Config cfg {
    .id = 1,
    .election_tick = 10,
    .heartbeat_tick = 3,
    .storage = std::make_shared<craft::MemoryStorage>(),
    .max_size_per_msg = 1024 * 1024 * 1024,
    .max_inflight_msgs = 256,
  };
  auto rn = craft::RawNode::Start(cfg, {craft::Peer{1}});
  return 0;
}