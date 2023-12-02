
#include "DSM.h"
#include "Directory.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"

#include <algorithm>
#include <chrono>
#include <iostream>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;
thread_local LocalAllocator DSM::local_allocator;
thread_local MultiAllocator DSM::multi_allocator;
thread_local uint64_t DSM::thread_tag = 0;

#ifdef SMART
thread_local SmartLocalAllocator DSM::local_allocators[MEMORY_NODE_NUM]
                                                      [NR_DIRECTORY];
thread_local RdmaBuffer DSM::rbuf[MAX_CORO_NUM];
#else
thread_local RdmaBuffer DSM::rbuf[define::kMaxCoro];
#endif

DSM *DSM::getInstance(const DSMConfig &conf) {
  static DSM *dsm = nullptr;
  static WRLock lock;

  lock.wLock();
  if (!dsm) {
    dsm = new DSM(conf);
  } else {
  }
  lock.wUnlock();

  return dsm;
}

DSM::DSM(const DSMConfig &conf)
    : conf(conf), appID(0), cache(conf.cacheConfig),
      memThreadCount(conf.memThreadCount) {
  index_type = conf.index_type;
  baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);

  Debug::notifyInfo("shared memory size: %dGB, 0x%lx", conf.dsmSize, baseAddr);
  Debug::notifyInfo("cache size: %dGB", conf.cacheConfig.cacheSize);

  // warmup
  // memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
  for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
       i += 2 * define::MB) {
    *(char *)i = 0;
  }

  // clear up first chunk
  memset((char *)baseAddr, 0, define::kChunkSize);

  initRDMAConnection();

  // Build the directory and also startup the standbythread in memory node
  for (int i = 0; i < memThreadCount; ++i) {
    dirAgent[i] = new Directory(dirCon[i], remoteInfo, conf.machineNR, i,
                                myNodeID, memThreadCount);
  }

#ifdef COUNT_RDMA
  clear_rdma_statistic();
#endif

  // Use memcached for synchrounization for multiple nodes
  keeper->barrier("DSM-init");
}

DSM::~DSM() {}

void DSM::registerThread() {
  static bool has_init[MAX_APP_THREAD];
  if (thread_id != -1) {
    // std::cout << "Already register thread " << thread_id << std::endl;
    return;
  }

  thread_id = appID.fetch_add(1);
  // std::cout << "Newly register thread " << thread_id << std::endl;
  thread_tag = thread_id + (((uint64_t)this->getMyNodeID()) << 32) + 1;

  iCon = thCon[thread_id];

  if (!has_init[thread_id]) {
    iCon->message->initRecv();
    iCon->message->initSend();

    has_init[thread_id] = true;
  }

  // std::cout << "PerThreadRdmaBuf = " << define::kPerThreadRdmaBuf <<
  // std::endl; std::cout << "kPerCoroRdmaBuf = " << define::kPerCoroRdmaBuf <<
  // std::endl;
  if (index_type == 2) { // smart
    rdma_buffer = (char *)cache.data + thread_id * define::kPerThreadRdmaBuf;
  } else {
    rdma_buffer = (char *)cache.data + thread_id * 12 * define::MB;
  }
  //   std::cout << "PerCoroRdmaBuff = " << define::kPerCoroRdmaBuf <<
  //   std::endl; std::cout << "I am setting buffer in register thread " <<
  //   thread_id
  //             << std::endl;
  if (index_type == 2) {
    for (int i = 0; i < define::kMaxCoro; ++i) {
      rbuf[i].set_smart_buffer(rdma_buffer + i * define::kSmartPerCoroRdmaBuf);
    }
  } else {
    for (int i = 0; i < define::kMaxCoro; ++i) {
      rbuf[i].set_dex_buffer(rdma_buffer + i * define::kPerCoroRdmaBuf);
    }
  }
}

void DSM::initRDMAConnection() {

  Debug::notifyInfo("Machine NR: %d", conf.machineNR);

  // The current node keeps the remote information of every node in the cluster
  remoteInfo = new RemoteConnection[conf.machineNR];

  Debug::notifyInfo("Max_APP_THREAD: %d", MAX_APP_THREAD);
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    // Debug::notifyInfo("Current thread: %d", i);
    thCon[i] =
        new ThreadConnection(i, (void *)cache.data, cache.size * define::GB,
                             conf.machineNR, remoteInfo);
  }

  Debug::notifyInfo("NR_DIRECTORY: %d", NR_DIRECTORY);
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirCon[i] =
        new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                conf.machineNR, remoteInfo);
  }

  Debug::notifyInfo("Start Build the DSMKeeper");
  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.machineNR);

  myNodeID = keeper->getMyNodeID();
}

void DSM::read(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
               CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_read[getMyThreadID()][0]++;
  size_rdma_read[getMyThreadID()][0] += size;
#endif
  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_sync(char *buffer, GlobalAddress gaddr, size_t size,
                    CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  read(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_read[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::write(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_write[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += size;
#endif
  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  write(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_write[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::fill_keys_dest(RdmaOpRegion &ror, GlobalAddress gaddr, bool is_chip) {
  ror.lkey = iCon->cacheLKey;
  if (is_chip) {
    ror.dest = remoteInfo[gaddr.nodeID].lockBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].lockRKey[0];
  } else {
    ror.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
  }
}

void DSM::read_batch(RdmaOpRegion *rs, int k, bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_read[getMyThreadID()][0] += k;
  for (int i = 0; i < k; ++i) {
    size_rdma_read[getMyThreadID()][0] += rs[i].size;
  }
#endif
  int node_id = -1;
  for (int i = 0; i < k; ++i) {
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaReadBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaReadBatch(iCon->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  read_batch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_read[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::read_batches_sync(const std::vector<RdmaOpRegion> &rs,
                            CoroContext *ctx, int coro_id) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  RdmaOpRegion each_rs[MAX_MACHINE][kReadOroMax];
  int cnt[MAX_MACHINE];

  int i = 0;
  int k = rs.size();
  int poll_num = 0;
  while (i < k) {
    std::fill(cnt, cnt + MAX_MACHINE, 0);
    while (i < k) {
      int node_id = GlobalAddress{rs[i].dest}.nodeID;
      each_rs[node_id][cnt[node_id]++] = rs[i];
      i++;
      if (cnt[node_id] >= kReadOroMax)
        break;
    }
    for (int j = 0; j < MAX_MACHINE; ++j)
      if (cnt[j] > 0) {
        read_batch(each_rs[j], cnt[j], true, ctx);
        poll_num++;
      }
  }

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, poll_num, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_read[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::write_batch(RdmaOpRegion *rs, int k, bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_write[getMyThreadID()][0] += k;
  for (int i = 0; i < k; ++i) {
    size_rdma_write[getMyThreadID()][0] += rs[i].size;
  }
#endif
  int node_id = -1;
  for (int i = 0; i < k; ++i) {

    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  write_batch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_write[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::write_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx,
                             int coro_id) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  // auto& each_rs = write_batches_rs[coro_id];
  // auto& cnt = write_batches_cnt[coro_id];
  RdmaOpRegion each_rs[MAX_MACHINE][kWriteOroMax];
  int cnt[MAX_MACHINE];

  std::fill(cnt, cnt + MAX_MACHINE, 0);
  for (int i = 0; i < k; ++i) {
    int node_id = GlobalAddress{rs[i].dest}.nodeID;
    each_rs[node_id][cnt[node_id]++] = rs[i];
  }
  int poll_num = 0;
  for (int i = 0; i < MAX_MACHINE; ++i)
    if (cnt[i] > 0) {
      write_batch(each_rs[i], cnt[i], true, ctx);
      poll_num++;
    }

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, poll_num, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_write[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::write_faa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                    uint64_t add_val, bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_cas[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += 8;
#endif

  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;

    fill_keys_dest(faa_ror, gaddr, faa_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, signal);
  } else {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_faa_sync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                         uint64_t add_val, CoroContext *ctx) {
  write_faa(write_ror, faa_ror, add_val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_cas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                    uint64_t equal, uint64_t val, bool signal,
                    CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_cas[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += 8;
#endif

  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val,
                 signal);
  } else {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_cas_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, CoroContext *ctx) {
  write_cas(write_ror, cas_ror, equal, val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_read(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                   uint64_t equal, uint64_t val, bool signal,
                   CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_cas[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += 8;
#endif
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    fill_keys_dest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, signal);
  } else {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, true,
                ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_read_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                        uint64_t equal, uint64_t val, CoroContext *ctx) {
  cas_read(cas_ror, read_ror, equal, val, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSM::cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_cas[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += 8;
#endif
  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx) {
  cas(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  // std::cout << "equal = " << equal << std::endl;
  // std::cout << "rdma_buffer = " << *rdma_buffer << std::endl;
  return equal == *rdma_buffer;
}

// void DSM::cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                    uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//   rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                          remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                          equal, val, iCon->cacheLKey,
//                          remoteInfo[gaddr.nodeID].dsmRKey[0], mask, signal);
// }

// bool DSM::cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                         uint64_t *rdma_buffer, uint64_t mask) {
//   cas_mask(gaddr, equal, val, rdma_buffer, mask);
//   ibv_wc wc;
//   pollWithCQ(iCon->cq, 1, &wc);

//   return (equal & mask) == (*rdma_buffer & mask);
// }

// void DSM::faa_boundary(GlobalAddress gaddr, uint64_t add_val,
//                        uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                        CoroContext *ctx) {
//   if (ctx == nullptr) {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].dsmRKey[0], mask,
//                             signal);
//   } else {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].dsmRKey[0], mask, true,
//                             ctx->coro_id);
//     (*ctx->yield)(*ctx->master);
//   }
// }

// void DSM::faa_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                             uint64_t *rdma_buffer, uint64_t mask,
//                             CoroContext *ctx) {
//   faa_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//   if (ctx == nullptr) {
//     ibv_wc wc;
//     pollWithCQ(iCon->cq, 1, &wc);
//   }
// }

void DSM::read_dm(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
                  CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_read[getMyThreadID()][0]++;
  size_rdma_read[getMyThreadID()][0] += size;
#endif
  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_dm_sync(char *buffer, GlobalAddress gaddr, size_t size,
                       CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  read_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_read[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::write_dm(const char *buffer, GlobalAddress gaddr, size_t size,
                   bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_write[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += size;
#endif
  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1,
              signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_dm_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                        CoroContext *ctx) {
#ifdef COUNT_TIME
  auto start = std::chrono::high_resolution_clock::now();
#endif
  write_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
#ifdef COUNT_TIME
  auto end = std::chrono::high_resolution_clock::now();
  time_rdma_write[getMyThreadID()][0] +=
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
#endif
}

void DSM::cas_dm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {
#ifdef COUNT_RDMA
  num_rdma_cas[getMyThreadID()][0]++;
  size_rdma_write[getMyThreadID()][0] += 8;
#endif
  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], true,
                       ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_dm_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, CoroContext *ctx) {
  cas_dm(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

// void DSM::cas_dm_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                       uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//   rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                          remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                          equal, val, iCon->cacheLKey,
//                          remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
// }

// bool DSM::cas_dm_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                            uint64_t *rdma_buffer, uint64_t mask) {
//   cas_dm_mask(gaddr, equal, val, rdma_buffer, mask);
//   ibv_wc wc;
//   pollWithCQ(iCon->cq, 1, &wc);

//   return (equal & mask) == (*rdma_buffer & mask);
// }

// void DSM::faa_dm_boundary(GlobalAddress gaddr, uint64_t add_val,
//                           uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                           CoroContext *ctx) {
//   if (ctx == nullptr) {

//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].lockRKey[0], mask,
//                             signal);
//   } else {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].lockRKey[0], mask, true,
//                             ctx->coro_id);
//     (*ctx->yield)(*ctx->master);
//   }
// }

// void DSM::faa_dm_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                                uint64_t *rdma_buffer, uint64_t mask,
//                                CoroContext *ctx) {
//   faa_dm_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//   if (ctx == nullptr) {
//     ibv_wc wc;
//     pollWithCQ(iCon->cq, 1, &wc);
//   }
// }

uint64_t DSM::poll_rdma_cq(int count) {
  ibv_wc wc;
  pollWithCQ(iCon->cq, count, &wc);

  return wc.wr_id;
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id) {
  ibv_wc wc;
  int res = pollOnce(iCon->cq, 1, &wc);

  wr_id = wc.wr_id;

  return res == 1;
}