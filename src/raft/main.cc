#include <iostream>
#include <vector>
#include <map>

#include "raft/storage.h"

int main(int argc, char* argv[]) {
    std::vector<raftpb::Entry> ents;
    for (uint64_t i = 1; i <= 3; i++) {
        raftpb::Entry ent;
        ent.set_index(i);
        ent.set_term(i);
        ents.push_back(ent);
    }

    craft::MemoryStorage storage;

    std::cerr << "----------------------------2" << std::endl;

    craft::Status status = storage.Append(ents);

    std::cerr << "----------------------------3" << std::endl;
    std::cerr << status.Str() << std::endl;

    // uint64_t term;
    // craft::Status status = storage.Term(1, term);
    // if (status.ok()) {

    // } else {
    //     std::cerr << status.Str() << std::endl;
    // }
}