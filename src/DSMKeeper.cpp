#include "DSMKeeper.h"

#include "Connection.h"
#include <iostream>

const char *DSMKeeper::OK = "OK";
const char *DSMKeeper::ServerPrefix = "SPre";

void DSMKeeper::initLocalMeta() {
  // Following information is shared among Directories in one node
  // And then each directory(memory region)/App(compute region) partition the
  // following area among threads (e.g., APP, Directory)
  localMeta.dsmBase = (uint64_t)dirCon[0]->dsmPool;
  localMeta.lockBase = (uint64_t)dirCon[0]->lockPool;
  localMeta.cacheBase = (uint64_t)thCon[0]->cachePool;

  // per thread APP
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    localMeta.appTh[i].lid = thCon[i]->ctx.lid;
    localMeta.appTh[i].rKey = thCon[i]->cacheMR->rkey;
    memcpy((char *)localMeta.appTh[i].gid, (char *)(&thCon[i]->ctx.gid),
           16 * sizeof(uint8_t));

    localMeta.appUdQpn[i] = thCon[i]->message->getQPN();
  }

  // per thread DIR
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    localMeta.dirTh[i].lid = dirCon[i]->ctx.lid;
    localMeta.dirTh[i].rKey = dirCon[i]->dsmMR->rkey;
    localMeta.dirTh[i].lock_rkey = dirCon[0]->lockMR->rkey;
    memcpy((char *)localMeta.dirTh[i].gid, (char *)(&dirCon[i]->ctx.gid),
           16 * sizeof(uint8_t));

    localMeta.dirUdQpn[i] = dirCon[i]->message->getQPN();
  }
}

bool DSMKeeper::connectNode(uint16_t remoteID) {

  setDataToRemote(remoteID);

  std::string setK = setKey(remoteID);
  memSet(setK.c_str(), setK.size(), (char *)(&localMeta), sizeof(localMeta));

  std::string getK = getKey(remoteID);
  ExchangeMeta *remoteMeta = (ExchangeMeta *)memGet(getK.c_str(), getK.size());

  setDataFromRemote(remoteID, remoteMeta);

  free(remoteMeta);
  return true;
}

void DSMKeeper::setDataToRemote(uint16_t remoteID) {
  // Memory node information (QPN)
  // for (int i = 0; i < NR_DIRECTORY; ++i) {
  for (int i = 0; i < 1; ++i) {
    auto &c = dirCon[i];
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      localMeta.dirRcQpn2app[i][k] = c->data2app[k][remoteID]->qp_num;
    }
  }

  // Compute node information (QPN)
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    auto &c = thCon[i];
    // for (int k = 0; k < NR_DIRECTORY; ++k) {
    for (int k = 0; k < 1; ++k) {
      localMeta.appRcQpn2dir[i][k] = c->data[k][remoteID]->qp_num;
    }
  }
}

/// @brief The real connection function: the current node gets all the required
/// information from the remote such as Rkey, gid, lid (used for routing) and
/// QPN (used for connection); then the current node is ready to connect to the
/// remote node
/// @param remoteID
/// @param remoteMeta
void DSMKeeper::setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta) {
  // Following two loops is to make RC between compute and memory nodes

  // The following loop modify the QP state in memory node (of this machine) to
  // make it(them) connect to the remoteID
  // for (int i = 0; i < NR_DIRECTORY; ++i) {
  for (int i = 0; i < 1; ++i) {
    auto &c = dirCon[i];

    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      auto &qp = c->data2app[k][remoteID]; // qp in the memory node

      assert(qp->qp_type == IBV_QPT_RC);
      modifyQPtoInit(qp, &c->ctx);
      modifyQPtoRTR(qp, remoteMeta->appRcQpn2dir[k][i],
                    remoteMeta->appTh[k].lid, remoteMeta->appTh[k].gid,
                    &c->ctx);
      modifyQPtoRTS(qp);
    }
  }

  // Connect the computing threads in this machine to the memory node of
  // remoteID
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    auto &c = thCon[i];
    // for (int k = 0; k < NR_DIRECTORY; ++k) {
    for (int k = 0; k < 1; ++k) {
      auto &qp = c->data[k][remoteID];

      assert(qp->qp_type == IBV_QPT_RC);
      modifyQPtoInit(qp, &c->ctx);
      modifyQPtoRTR(qp, remoteMeta->dirRcQpn2app[k][i],
                    remoteMeta->dirTh[k].lid, remoteMeta->dirTh[k].gid,
                    &c->ctx);
      modifyQPtoRTS(qp);
    }
  }

  auto &info = remoteCon[remoteID];
  info.dsmBase = remoteMeta->dsmBase;
  info.cacheBase = remoteMeta->cacheBase;
  info.lockBase = remoteMeta->lockBase;

  // The following two loop is to create Ah for UD connection for RPC

  // Store the memory pool Rkey etc. information of remoteID in the current
  // machine; we don't need to modify the QP state? the QP state has been
  // changed to RTS when it is created because in UD, we don't need the remote
  // GID/LID/QPN to attach to the QP context, instead, we use the Ah for
  // communication
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    info.dsmRKey[i] = remoteMeta->dirTh[i].rKey;
    info.lockRKey[i] = remoteMeta->dirTh[0].lock_rkey;
    info.dirMessageQPN[i] = remoteMeta->dirUdQpn[i];

    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      struct ibv_ah_attr ahAttr;
      fillAhAttr(&ahAttr, remoteMeta->dirTh[i].lid, remoteMeta->dirTh[i].gid,
                 &thCon[k]->ctx);
      info.appToDirAh[k][i] = ibv_create_ah(thCon[k]->ctx.pd, &ahAttr);

      assert(info.appToDirAh[k][i]);
    }
  }

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    info.appRKey[i] = remoteMeta->appTh[i].rKey;
    info.appMessageQPN[i] = remoteMeta->appUdQpn[i];

    for (int k = 0; k < NR_DIRECTORY; ++k) {
      struct ibv_ah_attr ahAttr;
      fillAhAttr(&ahAttr, remoteMeta->appTh[i].lid, remoteMeta->appTh[i].gid,
                 &dirCon[k]->ctx);
      info.dirToAppAh[k][i] = ibv_create_ah(dirCon[k]->ctx.pd, &ahAttr);

      assert(info.dirToAppAh[k][i]);
    }
  }
}

void DSMKeeper::connectMySelf() {
  setDataToRemote(getMyNodeID());
  setDataFromRemote(getMyNodeID(), &localMeta);
}

void DSMKeeper::initRouteRule() {

  std::string k =
      std::string(ServerPrefix) + std::to_string(this->getMyNodeID());
  memSet(k.c_str(), k.size(), getMyIP().c_str(), getMyIP().size());
}

void DSMKeeper::barrier(const std::string &barrierKey) {
  std::string key = std::string("barrier-") + barrierKey;
  if (this->getMyNodeID() == 0) {
    memSet(key.c_str(), key.size(), "0", 1);
  }
  memFetchAndAdd(key.c_str(), key.size());
  while (true) {
    uint64_t v = std::stoull(memGet(key.c_str(), key.size()));
    if (v == this->getServerNR()) {
      return;
    }
  }
}

void DSMKeeper::barrier(const std::string &barrierKey, int num) {
  std::string key = std::string("barrier-") + barrierKey;
  if (this->getMyNodeID() == 0) {
    memSet(key.c_str(), key.size(), "0", 1);
  }
  memFetchAndAdd(key.c_str(), key.size());
  while (true) {
    uint64_t v = std::stoull(memGet(key.c_str(), key.size()));
    if (v == num) {
      return;
    }
  }
}

uint64_t DSMKeeper::sum(const std::string &sum_key, uint64_t value,
                        bool time_out) {
  // std::cout << "in this funciton" << std::endl;
  return sum(sum_key, value, this->getServerNR(), time_out);
}

uint64_t DSMKeeper::sum(const std::string &sum_key, uint64_t value,
                        int node_num, bool time_out) {
  // std::cout << "in long parameter function" << std::endl;
  std::string key_prefix = std::string("sum-") + sum_key;

  std::string key = key_prefix + std::to_string(this->getMyNodeID());
  memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

  uint64_t ret = 0;
  // std::cout << "Node num = " << node_num << std::endl;
  for (int i = 0; i < node_num; ++i) {
    key = key_prefix + std::to_string(i);
    auto mem_ret = memGet(key.c_str(), key.size(), nullptr, time_out);
    if (mem_ret != nullptr) {
      ret += *(uint64_t *)mem_ret;
    }
  }

  return ret;
}

uint64_t DSMKeeper::min(const std::string &min_key, uint64_t value,
                        int node_num) {
  std::string key_prefix = std::string("min-") + min_key;

  std::string key = key_prefix + std::to_string(this->getMyNodeID());
  memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

  uint64_t ret = 0;
  uint64_t min = std::numeric_limits<uint64_t>::max();
  for (int i = 0; i < node_num; ++i) {
    key = key_prefix + std::to_string(i);
    auto mem_ret = memGet(key.c_str(), key.size(), nullptr, false);
    if (mem_ret != nullptr) {
      auto cur_val = *(uint64_t *)mem_ret;
      if (cur_val < min) {
        min = cur_val;
      }
    }
  }

  return min;
}