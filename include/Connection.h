#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

#include "DirectoryConnection.h"
#include "ThreadConnection.h"

struct RemoteConnection {
  // directory
  uint64_t dsmBase; // dsmBase of one remote node

  // Compute threads (App) can use following information to send requests to the
  // remote memory pool (Directory)
  uint32_t dsmRKey[NR_DIRECTORY];
  uint32_t dirMessageQPN[NR_DIRECTORY]; // QPN of the dir in one remote node
  ibv_ah
      *appToDirAh[MAX_APP_THREAD]
                 [NR_DIRECTORY]; // Store the address information of remote node

  // cache
  uint64_t cacheBase;

  // lock memory
  uint64_t lockBase;
  uint32_t lockRKey[NR_DIRECTORY];

  // app thread information, so the directory(memory pool can use below
  // information to send the message to the compute node)
  uint32_t appRKey[MAX_APP_THREAD];
  uint32_t appMessageQPN[MAX_APP_THREAD];
  ibv_ah *dirToAppAh[NR_DIRECTORY][MAX_APP_THREAD];
};

#endif /* __CONNECTION_H__ */
