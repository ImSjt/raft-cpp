#include "common/util.h"

namespace craft {

void Util::LimitSize(std::vector<raftpb::Entry>& ents, uint64_t max_size) {
    if (ents.empty()) {
        return;
    }

    uint64_t size = 0;
    for (auto it = ents.begin(); it != ents.end(); ++it) {
        size += static_cast<uint64_t>(it->ByteSizeLong());
        if (size > max_size) {
            ents.erase(it, ents.end());
            break;
        }
    }
}

} // namespace craft