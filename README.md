- etcd raft
- cpp17
- cmake?makefile?
- goggle test

git submodule add https://xxx
git submodule update --init --recursive
git submodule update --init xxx

git submodule update --remote liba

git config -f .gitmodules submodule.liba.branch dev
git submodule update --remote

git submodule rm xxx


TODO
- glog // done
- quorum // done
- google fmt
- tracker // done
- confchange
- storage
- unstable
- raftlog
- raft
- readonly