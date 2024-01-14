#pragma once

#include <stdarg.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs {

const int RAFT_RETRY_MS_BASE = 200;
const int RAFT_DISABLED_TIMER_INTERVAL = 1000;
const int RAFT_LEADER_PING_INTERVAL = 100;

enum class RaftTimerStatus { DISABLED, RESET, ENABLED };

enum class RaftRole { Follower, Candidate, Leader };

// TODO: maybe use this?
// class RaftTimer{
// private:
//   std::mutex mtx;
//   std::condition_variable cv;
//   RaftTimerStatus status;
//   int interval_ms;
//   bool random_timer;
//   std::uniform_int_distribution<int> dist;
//   // members for debug purposes
//   ThreadPool &thread_pool;

// public:
//   RaftTimer(int interval_ms,bool random_timer,RaftTimerStatus initial_status)
//   :status(initial_status),interval_ms(interval_ms),random_timer(random_timer){
//     if(random_timer){
//       dist=std::uniform_int_distribution<>(interval_ms,2*interval_ms);
//     }
//   }
//   void step(std::unique_lock<std::mutex> &lock,void (*work)()){

//   }
// };

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {
#define RAFT_LOG(fmt, args...)                                           \
  do {                                                                   \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(    \
                   std::chrono::system_clock::now().time_since_epoch())  \
                   .count();                                             \
    char buf[512];                                                       \
    sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, \
            __FILE__, __LINE__, my_id, current_term, role, ##args);      \
    thread_pool->enqueue([=]() { std::cerr << buf; });                   \
  } while (0);

 public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availablility);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

 private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
      -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::mutex leader_timer_mtx; /* A mutex to protect the timer status */
  std::condition_variable leader_timer_cv;
  std::mutex follower_timer_mtx;
  std::condition_variable follower_timer_cv;
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer>
      rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
      rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id;

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  std::uniform_int_distribution<> retry_ms_distrib =
      std::uniform_int_distribution<>(RAFT_RETRY_MS_BASE,
                                      3 * RAFT_RETRY_MS_BASE / 2);
  std::mt19937 rand_gen;

  bool voted;
  int voted_for;

  int commit_index;
  std::vector<unsigned char> has_voted_for_this;
  int candidate_vote_cnt;

  // leader's state
  std::vector<int> next_index;
  std::vector<int> match_index;

  RaftTimerStatus follower_timer_status = RaftTimerStatus::RESET;
  RaftTimerStatus leader_timer_status = RaftTimerStatus::DISABLED;

  static void set_timer(std::mutex &mtx, std::condition_variable &cv,
                        RaftTimerStatus &status, RaftTimerStatus new_status,
                        bool holding_mtx = false);
  void change_role(RaftRole new_role, bool caller_holding_leader_mtx = false,
                   bool caller_holding_follower_mtx = false);
  void change_term(int new_term);

  void send_append_entries_to(int target_id);

  // snapshot related
  std::vector<uint8_t> snapshot_buffer;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true),
      node_configs(configs),
      my_id(node_id),
      stopped(true),
      role(RaftRole::Follower),
      current_term(0),
      leader_id(-1) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */

  rpc_server->run(true, configs.size());

  has_voted_for_this.resize(configs.size());
  next_index.resize(configs.size());
  match_index.resize(configs.size());

  // random seed
  std::random_device rand_dev;
  rand_gen.seed(rand_dev());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  RAFT_LOG("destructing");
  stop();

  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_timer(std::mutex &mtx,
                                                std::condition_variable &cv,
                                                RaftTimerStatus &status,
                                                RaftTimerStatus new_status,
                                                bool holding_mtx) {
  if (holding_mtx) {
    status = new_status;
  } else {
    std::unique_lock<std::mutex> lock(mtx);
    status = new_status;
    lock.unlock();
    cv.notify_all();
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::change_role(
    RaftRole new_role, bool caller_holding_leader_mtx,
    bool caller_holding_follower_mtx) {
  // if (role == new_role) return;
  role = new_role;
  switch (new_role) {
    case RaftRole::Follower: {
      // TODO
      RAFT_LOG("Changing to follower");
      set_timer(follower_timer_mtx, follower_timer_cv, follower_timer_status,
                RaftTimerStatus::RESET, caller_holding_follower_mtx);
      set_timer(leader_timer_mtx, leader_timer_cv, leader_timer_status,
                RaftTimerStatus::DISABLED, caller_holding_leader_mtx);
      break;
    }
    case RaftRole::Leader: {
      // TODO
      RAFT_LOG("Changing to leader");
      int cluster_size = node_configs.size();
      for (size_t i = 0; i < cluster_size; i++) {
        next_index[i] = log_storage->entry_cnt() + 1;
        match_index[i] = 0;
      }
      set_timer(follower_timer_mtx, follower_timer_cv, follower_timer_status,
                RaftTimerStatus::DISABLED, caller_holding_follower_mtx);
      set_timer(leader_timer_mtx, leader_timer_cv, leader_timer_status,
                RaftTimerStatus::ENABLED,
                caller_holding_leader_mtx);  // immediately send pings
      break;
    }
    case RaftRole::Candidate: {
      // TODO
      RAFT_LOG("Changing to candidate");
      set_timer(follower_timer_mtx, follower_timer_cv, follower_timer_status,
                RaftTimerStatus::RESET, caller_holding_follower_mtx);
      set_timer(leader_timer_mtx, leader_timer_cv, leader_timer_status,
                RaftTimerStatus::DISABLED, caller_holding_leader_mtx);
      change_term(current_term + 1);
      candidate_vote_cnt = 1;
      voted = true;
      voted_for = my_id;
      this->log_storage->set_voted_for(my_id);
      int cluster_size = node_configs.size();
      for (size_t i = 0; i < cluster_size; i++) {
        has_voted_for_this[i] = false;
      }
      has_voted_for_this[my_id] = true;
      break;
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::change_term(int new_term) {
  current_term = new_term;
  this->log_storage->set_current_term(new_term);
  voted = false;
  this->log_storage->set_voted_for(-1);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries_to(int target_id) {
  // only leader needs to call this
  if (this->log_storage->need_install_snapshot(next_index[target_id])) {
    RAFT_LOG("sending install_snapshot to %d, idx: %d", target_id,
             next_index[target_id]);
    int last_included_index =
        this->log_storage->get_snapshot_last_included_index();
    int last_included_term =
        this->log_storage->get_snapshot_last_included_term();
    std::vector<u8> snapshot_data = this->log_storage->get_snapshot();
    send_install_snapshot(
        target_id,
        InstallSnapshotArgs{current_term, my_id, last_included_index,
                            last_included_term, 0, snapshot_data, true});
  } else {
    RAFT_LOG("sending append_entries to %d, idx:%d", target_id,
             next_index[target_id]);
    int prev_log_index = next_index[target_id] - 1;
    int prev_log_term = log_storage->get_entry(prev_log_index).term;
    int current_max_log_index = log_storage->entry_cnt();
    if (prev_log_index == current_max_log_index) return;
    std::vector<RaftLogEntry<Command>> log_entries;
    for (int idx = prev_log_index + 1; idx <= current_max_log_index; idx++) {
      log_entries.push_back(log_storage->get_entry(idx));
    }
    thread_pool->enqueue([=]() {
      send_append_entries(
          target_id, AppendEntriesArgs<Command>{
                         current_term, my_id, prev_log_index, prev_log_term,
                         log_entries, this->commit_index});
    });
  }
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  // rpc_server->run();
  thread_pool = std::make_unique<ThreadPool>(4);

  RAFT_LOG("*********starting*********");
  log_storage =
      std::make_unique<RaftLog<Command>>(std::make_shared<BlockManager>(
          "/tmp/raft_log/raft_data_" + std::to_string(my_id)));
  voted_for = log_storage->get_voted_for();
  voted = (voted_for != -1);
  current_term = log_storage->get_current_term();
  RAFT_LOG("recovered %d logs, voted_for: %d, current_term: %d",
           log_storage->entry_cnt(), voted_for, current_term);

  commit_index = 0;
  state = std::make_unique<StateMachine>();

  int snapshot_last_included_index =
      log_storage->get_snapshot_last_included_index();
  if (snapshot_last_included_index != 0) {
    // int snapshot_last_included_term =
    // log_storage->get_snapshot_last_included_term();
    RAFT_LOG("recovering from snapshot, commit_index: %d",
             snapshot_last_included_index);
    std::vector<u8> snapshot_data = log_storage->get_snapshot();
    state->apply_snapshot(snapshot_data);
    commit_index = snapshot_last_included_index;
  }

  snapshot_buffer.clear();

  for (int i = 0; i < node_configs.size(); i++) {
    rpc_clients_map[i] = std::make_unique<RpcClient>(
        node_configs[i].ip_address, node_configs[i].port, true);
  }
  stopped.store(false);

  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  RAFT_LOG("STOPPING");
  // std::unique_lock<std::mutex> leader_timer_lock(leader_timer_mtx);
  // std::unique_lock<std::mutex> follower_timer_lock(leader_timer_mtx);
  // RAFT_LOG("STOPPING--=-----");
  stopped.store(true);
  // leader_timer_lock.unlock();
  // leader_timer_cv.notify_all();
  // follower_timer_lock.unlock();
  // if(follower_timer_status==RaftTimerStatus::DISABLED)follower_timer_cv.notify_all();
  // if(leader_timer_status==RaftTimerStatus::DISABLED)follower_timer_cv.notify_all();
  background_ping->join();
  background_election->join();
  background_apply->join();
  background_commit->join();
  background_ping.reset();
  background_election.reset();
  background_apply.reset();
  background_commit.reset();
  thread_pool.reset();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  return std::make_tuple(role == RaftRole::Leader, current_term);
  // return std::make_tuple(false, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  RAFT_LOG("new_command received");
  std::unique_lock<std::mutex> lock(this->mtx);
  if (this->role != RaftRole::Leader) {
    RAFT_LOG("not leader, rejecting new_command");
    return std::make_tuple(false, current_term, -1);
  }
  Command command;
  command.deserialize(cmd_data, cmd_size);
  int index = this->log_storage->append_command(current_term, command);
  RAFT_LOG("new command appended at index: %d", index);
  return std::make_tuple(true, current_term, index);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  RAFT_LOG("saving snapshot");
  if (log_storage->need_install_snapshot(commit_index)) {
    // no need to create new snapshot
    return false;
  }
  std::vector<u8> snapshot_data = state->snapshot();
  int last_included_index = commit_index;
  int last_included_term = log_storage->get_entry(last_included_index).term;
  log_storage->set_snapshot_last_included_index_and_prune(last_included_index);
  log_storage->set_snapshot_last_included_term(last_included_term);
  log_storage->set_snapshot(snapshot_data);
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {
  /* Lab3: Your code here */
  RAFT_LOG("received request vote from node %d", args.candidate_id);
  std::unique_lock<std::mutex> lock(mtx);
  if (args.term < current_term) {
    return {current_term, false};
  }

  if (args.term > current_term) {
    change_term(args.term);
    change_role(RaftRole::Follower);
  }

  if (voted && voted_for != args.candidate_id) {
    return {current_term, false};
  }

  // TODO: check log up to date
  int last_log_term = log_storage->last_log_term();
  if (args.last_log_term > last_log_term ||
      (args.last_log_term == last_log_term &&
       args.last_log_index >= log_storage->entry_cnt())) {
    voted = true;
    voted_for = args.candidate_id;
    this->log_storage->set_voted_for(args.candidate_id);
    return RequestVoteReply{args.term, true};
  }

  return RequestVoteReply{current_term, false};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs arg, const RequestVoteReply reply) {
  /* Lab3: Your code here */
  RAFT_LOG("received request vote reply from %d, vote_granted: %s", target,
           (reply.vote_granted ? "true" : "false"));
  std::unique_lock<std::mutex> lock(this->mtx);
  if (role != RaftRole::Candidate) return;
  auto cluster_size = node_configs.size();
  if (reply.vote_granted) {
    if (!has_voted_for_this[target]) {
      candidate_vote_cnt++;
      has_voted_for_this[target] = true;
    }
    RAFT_LOG("current votes: %d", candidate_vote_cnt);
    // ATTENTION: majority is ">total/2"!!!
    if (candidate_vote_cnt > cluster_size / 2) {
      change_role(RaftRole::Leader);
    }
  } else {
    if (reply.term > current_term) {
      change_term(reply.term);
      change_role(RaftRole::Follower);
    }
  }
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */
  RAFT_LOG(
      "received append_entries from %d, term: %d, current_term on this node: "
      "%d ",
      rpc_arg.leader_id, rpc_arg.term, current_term);
  std::unique_lock<std::mutex> lock(this->mtx);
  AppendEntriesArgs<Command> arg =
      transform_rpc_append_entries_args<Command>(rpc_arg);
  if (arg.term < current_term) {
    RAFT_LOG("term too old, rejecting append_entries from %d",
             rpc_arg.leader_id);
    return AppendEntriesReply{current_term, false};
  }

  // TODO: real append
  RAFT_LOG("changing to follower and resetting timer");
  change_role(RaftRole::Follower);  // this will reset the timer, so no need to
                                    // write it again

  if (arg.term > current_term) {
    change_term(arg.term);
  }

  int entry_cnt = log_storage->entry_cnt();

  if (arg.prev_log_index != 0 &&
      (arg.prev_log_index > entry_cnt ||
       log_storage->get_entry(arg.prev_log_index).term != arg.prev_log_term)) {
    RAFT_LOG(
        "rejecting append_entries because of inconsistency, prev_log_index = "
        "%d, prev_log_term = %d",
        arg.prev_log_index, arg.prev_log_term);
    return AppendEntriesReply{current_term, false};
  }

  RAFT_LOG(
      "accepting append_entries, entries cnt: %d, current commit_index: %d, "
      "leader's commit index: %d, prev_log_index: %d, prev_log_term: %d, "
      "current entries cnt: %d",
      static_cast<int>(arg.log_entries.size()), commit_index,
      arg.leader_commit_index, arg.prev_log_index, arg.prev_log_term,
      entry_cnt);

  int current_log_index = arg.prev_log_index + 1;

  for (const auto &entry : arg.log_entries) {
    log_storage->set_entry(current_log_index, entry.term, entry.command);
    current_log_index++;
  }

  if (arg.leader_commit_index > commit_index) {
    RAFT_LOG("commiting entries from %d to %d", commit_index + 1,
             arg.leader_commit_index);
    for (int i = commit_index + 1; i <= arg.leader_commit_index; i++) {
      RaftLogEntry<Command> entry = log_storage->get_entry(i);
      Command command = entry.command;
      RAFT_LOG("applying log entry %d", i);
      state->apply_log(command);
    }
    commit_index = arg.leader_commit_index;
  }

  return AppendEntriesReply{current_term, true};

  // return AppendEntriesReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  /* Lab3: Your code here */
  RAFT_LOG("received append_entries reply");
  std::unique_lock<std::mutex> lock(this->mtx);
  if (reply.success) {
    match_index[node_id] =
        std::max(match_index[node_id],
                 arg.prev_log_index + static_cast<int>(arg.log_entries.size()));
    next_index[node_id] = match_index[node_id] + 1;
  } else {
    if (reply.term > current_term) {
      RAFT_LOG(
          "append_entries rejected because term is too old, changing to "
          "follower");
      change_term(reply.term);
      change_role(RaftRole::Follower);
      return;
    }
    // TODO: recursive send when inconsistency happens
    RAFT_LOG(
        "append_entries rejected because of inconsistency, changing "
        "next_index[%d] from %d to %d",
        node_id, next_index[node_id], arg.prev_log_index);
    if (arg.prev_log_index < next_index[node_id]) {
      next_index[node_id] = arg.prev_log_index;
      send_append_entries_to(node_id);
    }
  }

  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
  /* Lab3: Your code here */
  RAFT_LOG(
      "received install_snapshot from %d, last_included_index: %d, "
      "last_included_term: %d",
      args.leader_id, args.last_included_index, args.last_included_term);
  std::unique_lock<std::mutex> lock(this->mtx);
  if (args.term < current_term) {
    snapshot_buffer.clear();
    return InstallSnapshotReply{current_term};
  }
  change_role(RaftRole::Follower);  // the leader is alive
  if (args.term > current_term) {
    change_term(args.term);
  }
  if (args.offset == 0) {
    snapshot_buffer.clear();
  }
  size_t new_size = args.offset + args.data.size();
  snapshot_buffer.resize(new_size);
  for (size_t i = args.offset; i < new_size; i++) {
    snapshot_buffer[i] = args.data[i - args.offset];
  }
  if (!args.done) {
    return InstallSnapshotReply{current_term};
  }
  RaftLogEntry<Command> entry =
      log_storage->get_entry(args.last_included_index);
  if (entry.term == args.last_included_term) {
    log_storage->set_snapshot_last_included_index_and_prune(
        args.last_included_index);
    log_storage->set_snapshot_last_included_term(args.last_included_term);
    log_storage->set_snapshot(snapshot_buffer);
    snapshot_buffer.clear();
  } else {
    state->apply_snapshot(snapshot_buffer);
    log_storage->clear_entries();
    log_storage->set_snapshot_last_included_index(args.last_included_index);
    log_storage->set_snapshot_last_included_term(args.last_included_term);
    log_storage->set_snapshot(snapshot_buffer);
    commit_index = args.last_included_index;
  }

  return InstallSnapshotReply{current_term};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  if (reply.term > current_term) {
    change_term(reply.term);
    change_role(RaftRole::Follower);
    return;
  }
  if (arg.done) {
    next_index[node_id] = arg.last_included_index + 1;
    match_index[node_id] = arg.last_included_index;
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
  RAFT_LOG("sending request_vote to node %d", target_id);
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    // if (rpc_clients_map[target_id] != nullptr)
    RAFT_LOG("node %d not connected", target_id);

    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg,
                              res.unwrap()->as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
  RAFT_LOG("send_append_entries to %d, prev_log_index: %d", target_id,
           arg.prev_log_index);
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (this->role != RaftRole::Leader) return;
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    if (thread_pool)
      RAFT_LOG("send_append_entries: %d not connected or null", target_id);
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg,
                                res.unwrap()->as<AppendEntriesReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg,
                                  res.unwrap()->as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  /* Uncomment following code when you finish */
  std::unique_lock<std::mutex> follower_timer_lock(follower_timer_mtx);
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      // maybe this procedure can be encapsulated as a class?
      switch (follower_timer_status) {
        case RaftTimerStatus::DISABLED: {
          // RAFT_LOG("follower timer disabled");
          int ms_to_wait = RAFT_DISABLED_TIMER_INTERVAL;
          follower_timer_cv.wait_for(follower_timer_lock,
                                     std::chrono::milliseconds(ms_to_wait));
          continue;
        }
        case RaftTimerStatus::RESET: {
          follower_timer_status = RaftTimerStatus::ENABLED;
          int ms_to_wait = retry_ms_distrib(rand_gen);
          RAFT_LOG("follower timer reset, will be triggered after %dms",
                   ms_to_wait);
          follower_timer_cv.wait_for(follower_timer_lock,
                                     std::chrono::milliseconds(ms_to_wait));
          continue;
        }
        case RaftTimerStatus::ENABLED: {
          RAFT_LOG("follower timer triggered");
          change_role(RaftRole::Candidate, false, true);
          for (int i = 0; i < node_configs.size(); i++) {
            if (i == my_id) continue;
            RAFT_LOG("enqueing send_request_vote to node %d", i);
            thread_pool->enqueue([=]() {
              send_request_vote(
                  i,
                  RequestVoteArgs{current_term, my_id, log_storage->entry_cnt(),
                                  log_storage->last_log_term()});
            });
          }
          int ms_to_wait = retry_ms_distrib(rand_gen);
          follower_timer_cv.wait_for(follower_timer_lock,
                                     std::chrono::milliseconds(ms_to_wait));
          continue;
        }
      }
    }
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(RAFT_RETRY_MS_BASE));
      /* Lab3: Your code here */
      std::unique_lock<std::mutex> lock(this->mtx);
      if (role == RaftRole::Leader) {
        RAFT_LOG("run_background_commit timer triggered and I'm leader");
      } else {
        continue;
      }
      for (int i = 0; i < node_configs.size(); i++) {
        if (i == my_id) continue;
        send_append_entries_to(i);
      }
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.

  /* Uncomment following code when you finish */
  std::this_thread::sleep_for(
      std::chrono::milliseconds(RAFT_RETRY_MS_BASE / 2));
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::this_thread::sleep_for(
          std::chrono::milliseconds(RAFT_RETRY_MS_BASE));
      std::unique_lock<std::mutex> lock(this->mtx);
      if (role != RaftRole::Leader) {
        continue;
      }
      RAFT_LOG("run_background_apply timer triggered and I'm leader");
      int node_cnt = node_configs.size();
      for (int idx = log_storage->entry_cnt(); idx >= 1; idx--) {
        RaftLogEntry<Command> entry = log_storage->get_entry(idx);
        // no need to check older entries if this entry is not in this term
        if (entry.term != current_term) break;
        bool is_able_to_commit = false;
        int match_node_cnt = 0;
        for (int node_idx = 0; node_idx < node_cnt; node_idx++) {
          if (node_idx == my_id || match_index[node_idx] >= idx) {
            match_node_cnt++;
          }
          if (match_node_cnt > node_cnt / 2) {
            is_able_to_commit = true;
            break;
          }
        }
        if (is_able_to_commit) {
          if (idx > commit_index) {
            RAFT_LOG("log index %d is able to commit, updating commit_index",
                     idx);
            for (int index_to_apply = commit_index + 1; index_to_apply <= idx;
                 index_to_apply++) {
              RaftLogEntry<Command> entry =
                  log_storage->get_entry(index_to_apply);
              Command command = entry.command;
              RAFT_LOG("applying log entry %d", index_to_apply);
              state->apply_log(command);
            }
            commit_index = idx;
          }
          break;
        }
      }
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  std::unique_lock<std::mutex> leader_timer_lock(leader_timer_mtx);
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      switch (leader_timer_status) {
        case RaftTimerStatus::DISABLED: {
          // RAFT_LOG("leader timer disabled");
          int ms_to_wait = RAFT_DISABLED_TIMER_INTERVAL;
          leader_timer_cv.wait_for(leader_timer_lock,
                                   std::chrono::milliseconds(ms_to_wait));
          continue;
        }
        case RaftTimerStatus::RESET: {
          RAFT_LOG("leader timer reset");
          leader_timer_status = RaftTimerStatus::ENABLED;
          int ms_to_wait = RAFT_LEADER_PING_INTERVAL;
          leader_timer_cv.wait_for(leader_timer_lock,
                                   std::chrono::milliseconds(ms_to_wait));
          continue;
        }
        case RaftTimerStatus::ENABLED: {
          RAFT_LOG("leader timer triggered");
          RAFT_LOG("sending pings");
          for (int i = 0; i < node_configs.size(); i++) {
            if (i == my_id) continue;
            std::unique_lock<std::mutex> lock(this->mtx);
            thread_pool->enqueue([=]() {
              send_append_entries(i, AppendEntriesArgs<Command>{
                                         current_term,
                                         my_id,
                                         this->log_storage->entry_cnt(),
                                         this->log_storage->last_log_term(),
                                         {},
                                         this->commit_index});
            });

            // TODO: change prev_log_index prev_log_term to real value
            // TODO: change to recursive send
          }
          int ms_to_wait = RAFT_LEADER_PING_INTERVAL;
          leader_timer_cv.wait_for(leader_timer_lock,
                                   std::chrono::milliseconds(ms_to_wait));
          continue;
        }
      }
    }
  }

  return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    RAFT_LOG("DISABLING node %d", my_id);
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr) client.second.reset();
    }

    return;
  }
  RAFT_LOG("ENABLING node %d", my_id);

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id) target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}

}  // namespace chfs