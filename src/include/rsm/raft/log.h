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
  bool set_entry(int index, int term, const Command &command);
  RaftLogEntry<Command> get_entry(int index);

  int get_current_term();
  void set_current_term(int term);

  int get_voted_for();
  void set_voted_for(int voted_for);

  int get_snapshot_last_included_index();
  void set_snapshot_last_included_index(int snapshot_last_included_index);
  void set_snapshot_last_included_index_and_prune(
      int snapshot_last_included_index);

  int get_snapshot_last_included_term();
  void set_snapshot_last_included_term(int snapshot_last_included_term);

  std::vector<uint8_t> get_snapshot();
  void set_snapshot(const std::vector<uint8_t> &snapshot);

  // When the leader is about to send log entry at idx to a follower,
  // it should first check whether the follower has the log entry.
  // If not (it is pruned after the snapshot) then the leader should
  // send the snapshot to the follower.
  bool need_install_snapshot(int idx);

  // clear the entries (not affecting snapshot)
  void clear_entries();

  /* Lab3: Your code here */

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  std::vector<RaftLogEntry<Command>> entries;

  int snapshot_last_included_index;
  int snapshot_last_included_term;

  void set_reserved_block_int_value(int idx, int value);
  int get_reserved_block_int_value(int idx);

  int get_log_cnt();  // log count in vector
  void set_log_cnt(int cnt);

  int get_snapshot_len();
  void set_snapshot_len(int len);

  bool check_magic_number();

  static constexpr int MAGIC_NUMBER = 0x54464152;
  static constexpr int RESERVED_BLOCK_INT_CNT = 7;

  RaftLogEntry<Command> get_entry_from_bm(int idx);
  void write_entry_to_bm(int idx, const RaftLogEntry<Command> &entry);

  int physic_to_logic_idx(int idx);
  int logic_to_physic_idx(int idx);
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm) {
  /* Lab3: Your code here */
  entries.push_back(RaftLogEntry<Command>());
  if (!this->check_magic_number()) {  // not initialized
    this->set_reserved_block_int_value(0, MAGIC_NUMBER);
    this->set_current_term(0);
    this->set_voted_for(-1);
    this->set_log_cnt(0);
    this->set_snapshot_last_included_index(0);
    this->set_snapshot_last_included_term(0);
    this->set_snapshot_len(0);
  } else {
    this->snapshot_last_included_index = this->get_reserved_block_int_value(4);
    this->snapshot_last_included_term = this->get_reserved_block_int_value(5);
    int log_cnt = this->get_log_cnt();
    for (int i = 1; i <= log_cnt; i++) {
      entries.push_back(this->get_entry_from_bm(i));
    }
  }
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

template <typename Command>
int RaftLog<Command>::entry_cnt() {
  return entries.size() - 1 + this->get_snapshot_last_included_index();
}

template <typename Command>
int RaftLog<Command>::last_log_term() {
  if (entries.size() == 1) return this->get_snapshot_last_included_term();
  return entries[entries.size() - 1].term;
}

template <typename Command>
int RaftLog<Command>::append_command(int term, const Command &command) {
  entries.push_back(RaftLogEntry<Command>{term, command});
  write_entry_to_bm(entries.size() - 1, entries[entries.size() - 1]);
  set_log_cnt(entries.size() - 1);
  return entries.size() - 1 + this->get_snapshot_last_included_index();
}

template <typename Command>
bool RaftLog<Command>::set_entry(int index, int term, const Command &command) {
  int index_in_vector = logic_to_physic_idx(index);
  // shouldn't overwrite entries pruned after snapshot
  assert(index_in_vector > 0);
  if (index_in_vector > entries.size()) {
    return false;
  } else if (index_in_vector == entries.size()) {
    entries.push_back(RaftLogEntry<Command>{term, command});
    write_entry_to_bm(index_in_vector, entries[index_in_vector]);
    set_log_cnt(index_in_vector);
    return true;
  } else {
    // the following logs should all be discarded
    entries.resize(index_in_vector + 1);
    set_log_cnt(index_in_vector);
    entries[index_in_vector] = RaftLogEntry<Command>{term, command};
    write_entry_to_bm(index_in_vector, entries[index_in_vector]);
    return true;
  }
  return false;
}

template <typename Command>
RaftLogEntry<Command> RaftLog<Command>::get_entry(int index) {
  int index_in_vector = logic_to_physic_idx(index);
  if(index_in_vector==0){
    // prevent consistency check failure
    return RaftLogEntry<Command>{get_snapshot_last_included_term(), Command()};
  }
  if (index_in_vector > entries.size() || index_in_vector < 0) {
    return RaftLogEntry<Command>();
  } else
    return entries[index_in_vector];
}

template <typename Command>
void RaftLog<Command>::set_reserved_block_int_value(int idx, int value) {
  this->bm_->write_partial_block(0, reinterpret_cast<uint8_t *>(&value),
                                 idx * sizeof(int), sizeof(int));
}

template <typename Command>
int RaftLog<Command>::get_reserved_block_int_value(int idx) {
  int res;
  std::vector<uint8_t> buffer(this->bm_->block_size());
  this->bm_->read_block(0, buffer.data());
  std::memcpy(reinterpret_cast<uint8_t *>(&res),
              buffer.data() + idx * sizeof(int), sizeof(int));
  return res;
}

template <typename Command>
int RaftLog<Command>::get_current_term() {
  return this->get_reserved_block_int_value(1);
}

template <typename Command>
void RaftLog<Command>::set_current_term(int term) {
  this->set_reserved_block_int_value(1, term);
}

template <typename Command>
int RaftLog<Command>::get_voted_for() {
  return this->get_reserved_block_int_value(2);
}

template <typename Command>
void RaftLog<Command>::set_voted_for(int voted_for) {
  this->set_reserved_block_int_value(2, voted_for);
}

template <typename Command>
int RaftLog<Command>::get_log_cnt() {
  return this->get_reserved_block_int_value(3);
}

template <typename Command>
void RaftLog<Command>::set_log_cnt(int cnt) {
  this->set_reserved_block_int_value(3, cnt);
}

template <typename Command>
bool RaftLog<Command>::check_magic_number() {
  return this->get_reserved_block_int_value(0) == MAGIC_NUMBER;
}

template <typename Command>
RaftLogEntry<Command> RaftLog<Command>::get_entry_from_bm(int idx) {
  RaftLogEntry<Command> res;
  std::vector<uint8_t> buffer(this->bm_->block_size());
  uint8_t *buffer_data = buffer.data();
  this->bm_->read_block(idx, buffer_data);
  int *term = reinterpret_cast<int *>(buffer_data);
  int *command_len = reinterpret_cast<int *>(buffer_data + sizeof(int));
  std::vector<uint8_t> command_data(
      buffer.begin() + 2 * sizeof(int),
      buffer.begin() + 2 * sizeof(int) + *command_len);
  res.term = *term;
  res.command.deserialize(command_data, *command_len);
  return res;
}

template <typename Command>
void RaftLog<Command>::write_entry_to_bm(int idx,
                                         const RaftLogEntry<Command> &entry) {
  std::vector<uint8_t> buffer(this->bm_->block_size());
  uint8_t *buffer_data = buffer.data();
  std::memcpy(buffer_data, reinterpret_cast<const uint8_t *>(&entry.term),
              sizeof(int));
  std::vector<uint8_t> command_data =
      entry.command.serialize(entry.command.size());
  int command_len = static_cast<int>(command_data.size());
  std::memcpy(buffer_data + sizeof(int),
              reinterpret_cast<uint8_t *>(&command_len), sizeof(int));
  std::memcpy(buffer_data + 2 * sizeof(int), command_data.data(),
              command_data.size());
  this->bm_->write_block(idx, buffer_data);
}

template <typename Command>
int RaftLog<Command>::get_snapshot_last_included_index() {
  return this->snapshot_last_included_index;
}

template <typename Command>
void RaftLog<Command>::set_snapshot_last_included_index(
    int snapshot_last_included_index) {
  this->snapshot_last_included_index = snapshot_last_included_index;
  this->set_reserved_block_int_value(4, snapshot_last_included_index);
}

template <typename Command>
void RaftLog<Command>::set_snapshot_last_included_index_and_prune(
    int snapshot_last_included_index) {
  int old_snapshot_last_included_index =
      this->get_snapshot_last_included_index();
  if (snapshot_last_included_index < old_snapshot_last_included_index) {
    return;
  }
  int new_start_idx = logic_to_physic_idx(snapshot_last_included_index + 1);
  std::vector<RaftLogEntry<Command>> new_entries;
  new_entries.push_back(RaftLogEntry<Command>());
  for (int i = new_start_idx; i < entries.size(); i++) {
    new_entries.push_back(entries[i]);
  }
  entries = new_entries;
  int new_entries_size = new_entries.size() - 1;
  this->set_log_cnt(new_entries.size() - 1);
  for (int i = 1; i <= new_entries_size; i++) {
    write_entry_to_bm(i, entries[i]);
  }
  this->set_snapshot_last_included_index(snapshot_last_included_index);
}

template <typename Command>
int RaftLog<Command>::get_snapshot_last_included_term() {
  return this->snapshot_last_included_term;
}

template <typename Command>
void RaftLog<Command>::set_snapshot_last_included_term(
    int snapshot_last_included_term) {
  this->snapshot_last_included_term = snapshot_last_included_term;
  this->set_reserved_block_int_value(5, snapshot_last_included_term);
}

template <typename Command>
int RaftLog<Command>::physic_to_logic_idx(int idx) {
  return idx + this->get_snapshot_last_included_index();
}

template <typename Command>
int RaftLog<Command>::logic_to_physic_idx(int idx) {
  return idx - this->get_snapshot_last_included_index();
}

template <typename Command>
int RaftLog<Command>::get_snapshot_len() {
  return this->get_reserved_block_int_value(6);
}

template <typename Command>
void RaftLog<Command>::set_snapshot_len(int len) {
  this->set_reserved_block_int_value(6, len);
}

template <typename Command>
std::vector<uint8_t> RaftLog<Command>::get_snapshot() {
  int len = this->get_snapshot_len();
  if (len == 0) return {};
  std::vector<uint8_t> buffer(this->bm_->block_size());
  this->bm_->read_block(0, buffer.data());
  std::vector<uint8_t> res(
      buffer.begin() + RESERVED_BLOCK_INT_CNT * sizeof(int),
      buffer.begin() + RESERVED_BLOCK_INT_CNT * sizeof(int) + len);
  return res;
}

template <typename Command>
void RaftLog<Command>::set_snapshot(const std::vector<uint8_t> &snapshot) {
  int len = static_cast<int>(snapshot.size());
  this->set_snapshot_len(len);
  this->bm_->write_partial_block(0, snapshot.data(),
                                 RESERVED_BLOCK_INT_CNT * sizeof(int), len);
}

template <typename Command>
bool RaftLog<Command>::need_install_snapshot(int idx) {
  return idx <= this->get_snapshot_last_included_index();
}

template <typename Command>
void RaftLog<Command>::clear_entries() {
  entries.clear();
  entries.push_back(RaftLogEntry<Command>());
  this->set_log_cnt(0);
}

/* Lab3: Your code here */

} /* namespace chfs */
