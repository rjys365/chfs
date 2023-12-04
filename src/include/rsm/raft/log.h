#pragma once

#include <cstring>
#include <mutex>
#include <vector>

#include "block/manager.h"
#include "common/macros.h"

namespace chfs {

template <typename Command>
struct RaftLogEntry {
  int term;
  Command command;
};

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
 public:
  RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();
  int entry_cnt();
  int last_log_term();

  /* Lab3: Your code here */

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  std::vector<RaftLogEntry<Command>> entries;
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm) {
  /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

template <typename Command>
int RaftLog<Command>::entry_cnt(){
  return entries.size();
}

template <typename Command>
int RaftLog<Command>::last_log_term(){
  // TODO
  return 0;
}

/* Lab3: Your code here */

} /* namespace chfs */
