#ifndef __THREADCONNECTION_H__
#define __THREADCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnection;

// app thread
struct ThreadConnection {

  uint16_t threadID;

  RdmaContext ctx;
  ibv_cq *cq;     // for one-side verbs
  ibv_cq *rpc_cq; // receive cp for RPC; message internal has a standalone cp
                  // for send quque

  RawMessageConnection *message;

  // one-sided qp endpoint to each connected machine
  ibv_qp **data[NR_DIRECTORY]; // data[i][j] is QP connected to the ith
                               // directory in jth memory node

  ibv_mr *cacheMR;
  void *cachePool;
  uint32_t cacheLKey;
  RemoteConnection *remoteInfo;

  ThreadConnection(uint16_t threadID, void *cachePool, uint64_t cacheSize,
                   uint32_t machineNR, RemoteConnection *remoteInfo);

  void sendMessage2Dir(RawMessage *m, uint16_t node_id, uint16_t dir_id = 0);
};

#endif /* __THREADCONNECTION_H__ */
