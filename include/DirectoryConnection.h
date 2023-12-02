#ifndef __DIRECTORYCONNECTION_H__
#define __DIRECTORYCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnection;

// directory thread
struct DirectoryConnection {
  uint16_t dirID;

  RdmaContext ctx;
  ibv_cq *cq; // receive cq for rpc and also one-sided operation, therefore,
              // complete queue can be shared between different qps

  RawMessageConnection *message;

  ibv_qp *
      *data2app[MAX_APP_THREAD]; // QPs of the current memory node for one
                                 // sided connection, data2app[i][j] means
                                 // to who: i is the ith thread, j is the
                                 // jth compute node (remote computing machine)

  ibv_mr *dsmMR;
  void *dsmPool;
  uint64_t dsmSize;
  uint32_t dsmLKey;

  ibv_mr *lockMR;
  void *lockPool; // address on-chip
  uint64_t lockSize;
  uint32_t lockLKey;

  RemoteConnection *remoteInfo;

  DirectoryConnection(uint16_t dirID, void *dsmPool, uint64_t dsmSize,
                      uint32_t machineNR, RemoteConnection *remoteInfo);

  void sendMessage2App(RawMessage *m, uint16_t node_id, uint16_t th_id);
};

#endif /* __DIRECTORYCONNECTION_H__ */
