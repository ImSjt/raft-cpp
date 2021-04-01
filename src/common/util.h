#ifndef __CRAFT_COMMON_UTIL_H__
#define __CRAFT_COMMON_UTIL_H__

#include <cstdint>
#include <vector>

#include "craft/raft.pb.h"

namespace craft {

class Util {
public:
    static void LimitSize(std::vector<raftpb::Entry>& ents, uint64_t max_size);

};

} // namespace craft

#endif // __CRAFT_COMMON_UTIL_H__