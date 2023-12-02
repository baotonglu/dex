#ifndef __DSM_H__
#define __DSM_H__

#include <atomic>

#include "Cache.h"
#include "Config.h"
#include "Connection.h"
#include "DSMKeeper.h"
#include "GlobalAddress.h"
#include "LocalAllocator.h"
#include "MultiAllocator.h"
#include "RdmaBuffer.h"
#include "smart_local_allocator.h"
#include <iostream>
#include <random>
#include <thread>

class DSMKeeper;
class Directory;

#define COUNT_RDMA 1
#define SMART 1
// #define COUNT_TIME 1

class DSM {

public:
  void registerThread();
  // clear the network resources for all threads
  void resetThread() { appID.store(0); }
  static DSM *getInstance(const DSMConfig &conf);

  uint16_t getMyNodeID() { return myNodeID; }
  uint16_t getMyThreadID() { return thread_id; }
  uint16_t getClusterSize() { return conf.machineNR; }
  uint64_t getThreadTag() { return thread_tag; }
  uint32_t getComputeNum() { return conf.computeNR; }

  // RDMA operations
  // buffer is registered memory
  void read(char *buffer, GlobalAddress gaddr, size_t size, bool signal = true,
            CoroContext *ctx = nullptr);
  void read_sync(char *buffer, GlobalAddress gaddr, size_t size,
                 CoroContext *ctx = nullptr);

  void read_batch(RdmaOpRegion *rs, int k, bool signal = true,
                  CoroContext *ctx = nullptr);
  void read_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);
  void read_batches_sync(const std::vector<RdmaOpRegion> &rs,
                         CoroContext *ctx = nullptr, int coro_id = 0);

  void write(const char *buffer, GlobalAddress gaddr, size_t size,
             bool signal = true, CoroContext *ctx = nullptr);
  void write_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                  CoroContext *ctx = nullptr);

  void write_batch(RdmaOpRegion *rs, int k, bool signal = true,
                   CoroContext *ctx = nullptr);
  void write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);
  void write_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr,
                          int coro_id = 0);

  void write_faa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                 uint64_t add_val, bool signal = true,
                 CoroContext *ctx = nullptr);
  void write_faa_sync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                      uint64_t add_val, CoroContext *ctx = nullptr);

  void write_cas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror, uint64_t equal,
                 uint64_t val, bool signal = true, CoroContext *ctx = nullptr);
  void write_cas_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                      uint64_t equal, uint64_t val, CoroContext *ctx = nullptr);

  void cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
           uint64_t *rdma_buffer, bool signal = true,
           CoroContext *ctx = nullptr);
  bool cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                uint64_t *rdma_buffer, CoroContext *ctx = nullptr);

  void cas_read(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror, uint64_t equal,
                uint64_t val, bool signal = true, CoroContext *ctx = nullptr);
  bool cas_read_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                     uint64_t equal, uint64_t val, CoroContext *ctx = nullptr);

  // void cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
  //               uint64_t *rdma_buffer, uint64_t mask = ~(0ull),
  //               bool signal = true);
  // bool cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
  //                    uint64_t *rdma_buffer, uint64_t mask = ~(0ull));

  // void faa_boundary(GlobalAddress gaddr, uint64_t add_val,
  //                   uint64_t *rdma_buffer, uint64_t mask = 63,
  //                   bool signal = true, CoroContext *ctx = nullptr);
  // void faa_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
  //                        uint64_t *rdma_buffer, uint64_t mask = 63,
  //                        CoroContext *ctx = nullptr);

  // for on-chip device memory
  void read_dm(char *buffer, GlobalAddress gaddr, size_t size,
               bool signal = true, CoroContext *ctx = nullptr);
  void read_dm_sync(char *buffer, GlobalAddress gaddr, size_t size,
                    CoroContext *ctx = nullptr);

  void write_dm(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal = true, CoroContext *ctx = nullptr);
  void write_dm_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroContext *ctx = nullptr);

  void cas_dm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, bool signal = true,
              CoroContext *ctx = nullptr);
  bool cas_dm_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx = nullptr);

  void cas_dm_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, uint64_t mask = ~(0ull),
                   bool signal = true);
  bool cas_dm_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                        uint64_t *rdma_buffer, uint64_t mask = ~(0ull));
  void faa_dm_boundary(GlobalAddress gaddr, uint64_t add_val,
                       uint64_t *rdma_buffer, uint64_t mask = 63,
                       bool signal = true, CoroContext *ctx = nullptr);
  void faa_dm_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
                            uint64_t *rdma_buffer, uint64_t mask = 63,
                            CoroContext *ctx = nullptr);

  uint64_t poll_rdma_cq(int count = 1);
  bool poll_rdma_cq_once(uint64_t &wr_id);

  // Create a random number
  uint32_t get_random_id(int id) {
    static thread_local std::mt19937 *generator = nullptr;
    if (!generator)
      generator = new std::mt19937(clock() + pthread_self());
    static thread_local std::uniform_int_distribution<int> distribution(
        0, conf.machineNR - 1);

    auto idx = distribution(*generator);
    while (idx == id) {
      idx = distribution(*generator);
    }
    return idx;
  }

  uint64_t sum(uint64_t value) {
    static uint64_t count = 0;
    return keeper->sum(std::string("sum-") + std::to_string(count++), value,
                       true);
  }

  uint64_t sum(uint64_t value, int node_num) {
    static uint64_t count = 0;
    return keeper->sum(std::string("sum-") + std::to_string(count++), value,
                       node_num, true);
  }

  uint64_t sum_with_prefix(std::string prefix, uint64_t value, int node_num) {
    // std::cout << "Node num = " << node_num << std::endl;
    return keeper->sum(prefix, value, node_num, true);
  }

  uint64_t sum_total(uint64_t value, bool time_out) {
    static uint64_t count = 0;
    return keeper->sum(std::string("sum-total") + std::to_string(count++),
                       value, time_out);
  }

  uint64_t sum_total(uint64_t value, int node_num, bool time_out) {
    static uint64_t count = 0;
    return keeper->sum(std::string("sum-total") + std::to_string(count++),
                       value, node_num, time_out);
  }

  uint64_t min_total(uint64_t value, int node_num) {
    static uint64_t count = 0;
    return keeper->min(std::string("min-total") + std::to_string(count++),
                       value, node_num);
  }

  // Memcached operations for sync
  size_t Put(uint64_t key, const void *value, size_t count) {

    std::string k = std::string("gam-") + std::to_string(key);
    keeper->memSet(k.c_str(), k.size(), (char *)value, count);
    return count;
  }

  size_t Get(uint64_t key, void *value) {

    std::string k = std::string("gam-") + std::to_string(key);
    size_t size;
    char *ret = keeper->memGet(k.c_str(), k.size(), &size);
    memcpy(value, ret, size);

    return size;
  }

#ifdef COUNT_RDMA

  uint64_t get_rdma_read_num() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += num_rdma_read[i][0];
    }
    return total;
  }

  uint64_t get_rdma_write_num() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += num_rdma_write[i][0];
    }
    return total;
  }

  uint64_t get_rdma_read_size() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += size_rdma_read[i][0];
    }
    return total;
  }

  uint64_t get_rdma_write_size() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += size_rdma_write[i][0];
    }
    return total;
  }

  uint64_t get_rdma_write_time() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += time_rdma_write[i][0];
    }
    return total;
  }

  uint64_t get_rdma_read_time() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += time_rdma_read[i][0];
    }
    return total;
  }

  uint64_t get_rdma_cas_num() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += num_rdma_cas[i][0];
    }
    return total;
  }

  uint64_t get_rdma_rpc_num() {
    uint64_t total = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total += num_rdma_rpc[i][0];
    }
    return total;
  }

  void clear_rdma_statistic() {
    memset(reinterpret_cast<void *>(num_rdma_read), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(time_rdma_read), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(num_rdma_write), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(time_rdma_write), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(num_rdma_cas), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(num_rdma_rpc), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(size_rdma_read), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
    memset(reinterpret_cast<void *>(size_rdma_write), 0,
           sizeof(uint64_t) * MAX_APP_THREAD * 8);
  }

  // RDMA statistic
  uint64_t num_rdma_read[MAX_APP_THREAD][8];
  uint64_t time_rdma_read[MAX_APP_THREAD][8];
  uint64_t num_rdma_write[MAX_APP_THREAD][8];
  uint64_t time_rdma_write[MAX_APP_THREAD][8];
  uint64_t num_rdma_cas[MAX_APP_THREAD][8];
  uint64_t num_rdma_rpc[MAX_APP_THREAD][8];
  uint64_t size_rdma_read[MAX_APP_THREAD][8];
  uint64_t size_rdma_write[MAX_APP_THREAD][8];
#endif

private:
  DSM(const DSMConfig &conf);
  ~DSM();

  void initRDMAConnection();
  void fill_keys_dest(RdmaOpRegion &ror, GlobalAddress addr, bool is_chip);

  DSMConfig conf;
  std::atomic_int appID;
  Cache cache;
  int index_type;

  static thread_local int thread_id;
  static thread_local ThreadConnection *iCon;
  static thread_local char *rdma_buffer;
  static thread_local LocalAllocator local_allocator;
  static thread_local MultiAllocator multi_allocator;
  static thread_local uint64_t thread_tag;
#ifdef SMART
  static thread_local SmartLocalAllocator local_allocators[MEMORY_NODE_NUM]
                                                          [NR_DIRECTORY];
  static thread_local RdmaBuffer rbuf[MAX_CORO_NUM];
#else
  static thread_local RdmaBuffer rbuf[define::kMaxCoro];
#endif

  uint64_t baseAddr;
  uint32_t myNodeID;
  int memThreadCount;

  RemoteConnection *remoteInfo;
  ThreadConnection *thCon[MAX_APP_THREAD];
  DirectoryConnection *dirCon[NR_DIRECTORY];
  DSMKeeper *keeper;

  Directory *dirAgent[NR_DIRECTORY];

public:
  bool is_register() { return thread_id != -1; }
  void barrier(const std::string &ss) { keeper->barrier(ss); }
  void barrier(const std::string &ss, int node_num) {
    keeper->barrier(ss, node_num);
  }

  char *get_rdma_buffer() { return rdma_buffer; }
  RdmaBuffer &get_rbuf(int coro_id) { return rbuf[coro_id]; }

  GlobalAddress alloc(size_t size);
  GlobalAddress alloc(size_t size, uint32_t node_id);

#ifdef SMART
  // whether to do align
  GlobalAddress smart_alloc(size_t size, bool align = true);
  void smart_alloc_nodes(int node_num, GlobalAddress *addrs, bool align = true);
#endif

  int rpc_lookup(GlobalAddress start_node, uint64_t k, uint64_t &result);
  int rpc_update(GlobalAddress start_node, uint64_t k, uint64_t result,
                 GlobalAddress &leaf);
  int rpc_insert(GlobalAddress start_node, uint64_t k, uint64_t v,
                 GlobalAddress &leaf);
  int rpc_remove(GlobalAddress start_node, uint64_t k, GlobalAddress &leaf);

  void free(GlobalAddress addr);
  void smart_free(const GlobalAddress &addr, int size);

  void rpc_call_dir(const RawMessage &m, uint16_t node_id,
                    uint16_t dir_id = 0) {
#ifdef COUNT_RDMA
    num_rdma_rpc[getMyThreadID()][0]++;
    size_rdma_read[getMyThreadID()][0] += sizeof(RawMessage);
#endif

    auto buffer = (RawMessage *)iCon->message->getSendPool();

    memcpy(buffer, &m, sizeof(RawMessage));
    buffer->node_id = myNodeID;
    buffer->app_id = thread_id;

    iCon->sendMessage2Dir(buffer, node_id, dir_id);
  }

  RawMessage *rpc_wait() {
    ibv_wc wc;

    pollWithCQ(iCon->rpc_cq, 1, &wc);
    return (RawMessage *)iCon->message->getMessage();
  }
};

inline GlobalAddress DSM::alloc(size_t size) {
  thread_local int next_target_node =
      (getMyThreadID() + getMyNodeID()) % conf.machineNR;
  thread_local int next_target_dir_id =
      (getMyThreadID() + getMyNodeID()) % memThreadCount;

  bool need_chunk = false;
  auto addr = local_allocator.malloc(size, need_chunk);
  if (need_chunk) {
    RawMessage m;
    m.type = RpcType::MALLOC;

    this->rpc_call_dir(m, next_target_node, next_target_dir_id);
    local_allocator.set_chunck(rpc_wait()->addr);

    if (++next_target_dir_id == memThreadCount) {
      next_target_node = (next_target_node + 1) % conf.machineNR;
      next_target_dir_id = 0;
    }

    // retry
    addr = local_allocator.malloc(size, need_chunk);
  }

  return addr;
}

inline GlobalAddress DSM::alloc(size_t size, uint32_t node_id) {
  node_id = node_id % conf.machineNR;
  thread_local int next_target_dir_id =
      (getMyThreadID() + getMyNodeID()) % memThreadCount;

  assert((node_id < conf.machineNR) && (node_id >= 0));
  // std::cout << "node_id = " << node_id << std::endl;
  // std::cout << "dir_id = " << next_target_dir_id << std::endl;

  bool need_chunk = false;
  auto addr = multi_allocator.malloc(size, need_chunk, node_id);
  if (need_chunk) {
    RawMessage m;
    m.type = RpcType::MALLOC;
    // std::cout << "RPC for allocate nodes" << std::endl;
    this->rpc_call_dir(m, node_id, next_target_dir_id);
    multi_allocator.set_chunck(rpc_wait()->addr, node_id);
    // std::cout << "Return from allocation" << std::endl;

    if (++next_target_dir_id == memThreadCount) {
      next_target_dir_id = 0;
    }

    // retry
    addr = multi_allocator.malloc(size, need_chunk, node_id);
  }
  return addr;
}

#ifdef SMART

inline GlobalAddress DSM::smart_alloc(size_t size, bool align) {
  // thread_local int cur_target_node =
  //     (this->getMyThreadID() + this->getMyNodeID()) % MEMORY_NODE_NUM;
  thread_local int cur_target_node =
      (this->getMyThreadID() + this->getMyNodeID()) % conf.machineNR;
  thread_local int cur_target_dir_id =
      (this->getMyThreadID() + this->getMyNodeID()) % NR_DIRECTORY;
  if (++cur_target_dir_id == NR_DIRECTORY) {
    // cur_target_node = (cur_target_node + 1) % MEMORY_NODE_NUM;
    cur_target_node = (cur_target_node + 1) % conf.machineNR;
    cur_target_dir_id = 0;
  }

  // std::cout << "target node = " << cur_target_node
  //           << "; target dir = " << cur_target_dir_id << std::endl;

  auto &smart_local_allocator =
      local_allocators[cur_target_node][cur_target_dir_id];

  // alloc from the target node
  bool need_chunk = true;
  GlobalAddress addr = smart_local_allocator.malloc(size, need_chunk, align);
  if (need_chunk) {
    RawMessage m;
    m.type = RpcType::MALLOC;
    // std::cout << "need chunk and send a new message" << std::endl;
    this->rpc_call_dir(m, cur_target_node, cur_target_dir_id);
    smart_local_allocator.set_chunck(rpc_wait()->addr);
    // std::cout << "get chunk from remote" << std::endl;

    // retry
    addr = smart_local_allocator.malloc(size, need_chunk, align);
  }
  return addr;
}

inline void DSM::smart_alloc_nodes(int node_num, GlobalAddress *addrs,
                                   bool align) {
  for (int i = 0; i < node_num; ++i) {
    addrs[i] = smart_alloc(define::allocationPageSize, align);
  }
}

#endif

inline int DSM::rpc_lookup(GlobalAddress start_node, uint64_t k,
                           uint64_t &result) {
  RawMessage m;
  m.type = RpcType::LOOKUP;
  m.k = k;
  m.addr = start_node;

  thread_local uint16_t dir_id = pthread_self() % memThreadCount;
  // std::cout << start_node.nodeID << " push-down ID = " << dir_id <<
  // std::endl;
  this->rpc_call_dir(m, start_node.nodeID, dir_id);
  dir_id = (dir_id + 1) % memThreadCount;

  auto mm = rpc_wait();
  if (mm->level >= 1) {
    result = mm->addr.val;
  }
  return mm->level;
}

inline int DSM::rpc_update(GlobalAddress start_node, uint64_t k, uint64_t v,
                           GlobalAddress &leaf_addr) {
  RawMessage m;
  m.type = RpcType::UPDATE;
  m.k = k;
  m.v = v;
  m.addr = start_node;

  thread_local uint16_t dir_id = pthread_self() % memThreadCount;
  // std::cout << start_node.nodeID << " push-down ID = " << dir_id <<
  // std::endl;
  this->rpc_call_dir(m, start_node.nodeID, dir_id);
  dir_id = (dir_id + 1) % memThreadCount;

  auto mm = rpc_wait();
  leaf_addr = mm->addr;
  return mm->level;
}

inline int DSM::rpc_remove(GlobalAddress start_node, uint64_t k,
                           GlobalAddress &leaf_addr) {
  RawMessage m;
  m.type = RpcType::DELETE;
  m.k = k;
  m.addr = start_node;

  thread_local uint16_t dir_id = pthread_self() % memThreadCount;
  // std::cout << start_node.nodeID << " push-down ID = " << dir_id <<
  // std::endl;
  this->rpc_call_dir(m, start_node.nodeID, dir_id);
  dir_id = (dir_id + 1) % memThreadCount;

  auto mm = rpc_wait();
  leaf_addr = mm->addr;
  return mm->level;
}

inline int DSM::rpc_insert(GlobalAddress start_node, uint64_t k, uint64_t value,
                           GlobalAddress &leaf_addr) {
  RawMessage m;
  m.type = RpcType::INSERT;
  m.k = k;
  m.v = value;
  m.addr = start_node;

  thread_local uint16_t dir_id = pthread_self() % memThreadCount;
  this->rpc_call_dir(m, start_node.nodeID, dir_id);
  dir_id = (dir_id + 1) % memThreadCount;

  auto mm = rpc_wait();
  leaf_addr = mm->addr;
  return mm->level;
}

inline void DSM::free(GlobalAddress addr) { local_allocator.free(addr); }

#ifdef SMART
inline void DSM::smart_free(const GlobalAddress &addr, int size) {
  local_allocators[addr.nodeID][0].free(addr, size);
}
#endif

#endif /* __DSM_H__ */
