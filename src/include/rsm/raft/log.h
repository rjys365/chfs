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
  int append_command(int term, const Command &command);
  bool set_entry(int index,int term,const Command & command);
  RaftLogEntry<Command> get_entry(int index);

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
  entries.push_back(RaftLogEntry<Command>());
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

template <typename Command>
int RaftLog<Command>::entry_cnt(){
  return entries.size()-1;
}

template <typename Command>
int RaftLog<Command>::last_log_term(){
  // TODO
  if(entries.size()==1)return 0;
  return entries[entries.size()-1].term;
}

template <typename Command>
int RaftLog<Command>::append_command(int term, const Command &command){
  entries.push_back(RaftLogEntry<Command>{term,command});
  return entries.size()-1;
}

template <typename Command>
bool RaftLog<Command>::set_entry(int index,int term,const Command & command){
  if(index>entries.size()){
    return false;
  }
  else if(index==entries.size()){
    entries.push_back(RaftLogEntry<Command>{term,command});
    return true;
  }
  else{
    // the following logs should all be discarded
    entries.resize(index+1);
    entries[index]=RaftLogEntry<Command>{term,command};
    return true;
  }
  return false;
}

template <typename Command>
RaftLogEntry<Command> RaftLog<Command>::get_entry(int index){
  if(index>this->entry_cnt()){
    return RaftLogEntry<Command>();
  }
  else return entries[index];
}

/* Lab3: Your code here */

} /* namespace chfs */
