#pragma once

#include <cstring>
#include <mutex>
#include <vector>

#include "block/manager.h"
#include "common/macros.h"

namespace chfs {

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
 public:
  RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();

  /* Lab3: Your code here */

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) {
  /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
