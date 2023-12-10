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

  /* Lab3: Your code here */

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  std::vector<RaftLogEntry<Command>> entries;

  void set_reserved_block_int_value(int idx, int value);
  int get_reserved_block_int_value(int idx);

  int get_log_cnt();
  void set_log_cnt(int cnt);

  bool check_magic_number();

  static constexpr int magic_number = 0x54464152;

  RaftLogEntry<Command> get_entry_from_bm(int idx);
  void write_entry_to_bm(int idx, const RaftLogEntry<Command> &entry);
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm) {
  /* Lab3: Your code here */
  entries.push_back(RaftLogEntry<Command>());
  if (!this->check_magic_number()) {  // not initialized
    this->set_reserved_block_int_value(0, magic_number);
    this->set_current_term(0);
    this->set_voted_for(-1);
    this->set_log_cnt(0);
  } else {
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
  return entries.size() - 1;
}

template <typename Command>
int RaftLog<Command>::last_log_term() {
  // TODO
  if (entries.size() == 1) return 0;
  return entries[entries.size() - 1].term;
}

template <typename Command>
int RaftLog<Command>::append_command(int term, const Command &command) {
  entries.push_back(RaftLogEntry<Command>{term, command});
  write_entry_to_bm(entries.size() - 1, entries[entries.size() - 1]);
  set_log_cnt(entries.size() - 1);
  return entries.size() - 1;
}

template <typename Command>
bool RaftLog<Command>::set_entry(int index, int term, const Command &command) {
  if (index > entries.size()) {
    return false;
  } else if (index == entries.size()) {
    entries.push_back(RaftLogEntry<Command>{term, command});
    write_entry_to_bm(index, entries[index]);
    set_log_cnt(index);
    return true;
  } else {
    // the following logs should all be discarded
    entries.resize(index + 1);
    set_log_cnt(index);
    entries[index] = RaftLogEntry<Command>{term, command};
    write_entry_to_bm(index, entries[index]);
    return true;
  }
  return false;
}

template <typename Command>
RaftLogEntry<Command> RaftLog<Command>::get_entry(int index) {
  if (index > this->entry_cnt()) {
    return RaftLogEntry<Command>();
  } else
    return entries[index];
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
  return this->get_reserved_block_int_value(0) == magic_number;
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

/* Lab3: Your code here */

} /* namespace chfs */
