#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
  /* Lab3: Your code here */
  int term, candidate_id, last_log_index, last_log_term;

  MSGPACK_DEFINE(term, candidate_id, last_log_index, last_log_term)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  int term;
  bool vote_granted;

  MSGPACK_DEFINE(term, vote_granted)
};

template <typename Command>
struct AppendEntriesArgs {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  std::vector<RaftLogEntry<Command>> log_entries;
  int leader_commit_index;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  std::vector<std::pair<int, std::vector<u8>>> serialized_log_entries;
  int leader_commit_index;

  MSGPACK_DEFINE(term, leader_id, prev_log_index, prev_log_term,
                 serialized_log_entries, leader_commit_index)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(
    const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  RpcAppendEntriesArgs res;
  res.term = arg.term;
  res.leader_id = arg.leader_id;
  res.prev_log_index = arg.prev_log_index;
  res.prev_log_term = arg.prev_log_term;
  res.leader_commit_index = arg.leader_commit_index;

  for (const auto &entry : arg.log_entries) {
    res.serialized_log_entries.push_back(std::make_pair(
        entry.term, entry.command.serialize(entry.command.size())));
  }
  return res;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(
    const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  AppendEntriesArgs<Command> res;
  res.term = rpc_arg.term;
  res.leader_id = rpc_arg.leader_id;
  res.prev_log_index = rpc_arg.prev_log_index;
  res.prev_log_term = rpc_arg.prev_log_term;
  res.leader_commit_index = rpc_arg.leader_commit_index;

  for (const auto &entry_pair : rpc_arg.serialized_log_entries) {
    RaftLogEntry<Command> entry;
    entry.term = entry_pair.first;
    entry.command.deserialize(entry_pair.second, entry_pair.second.size());
    res.log_entries.push_back(entry);
  }

  return res;
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  bool success;
  MSGPACK_DEFINE(term, success)
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

} /* namespace chfs */