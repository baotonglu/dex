#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <vector>

#include "Keeper.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;

struct ExPerThread {
  uint16_t lid;
  uint8_t gid[16];

  uint32_t rKey;

  uint32_t lock_rkey; // for directory on-chip memory
} __attribute__((packed));

struct ExchangeMeta {
  uint64_t dsmBase;
  uint64_t cacheBase;
  uint64_t lockBase;

  ExPerThread appTh[MAX_APP_THREAD]; // Rkey in computing node's read write
                                     // buffer, but not sure whether it is
                                     // useful to transfer this to remote node
  ExPerThread dirTh[NR_DIRECTORY];   // Rkey in memory node's memory pool

  uint32_t appUdQpn[MAX_APP_THREAD]; // computing node's rpc QPN
  uint32_t dirUdQpn[NR_DIRECTORY];   // memoy node's rpc QPN

  uint32_t appRcQpn2dir[MAX_APP_THREAD][NR_DIRECTORY]; // QPNs in compute node

  uint32_t dirRcQpn2app[NR_DIRECTORY][MAX_APP_THREAD]; // OPNs in memory node

} __attribute__((packed));

class DSMKeeper : public Keeper {

private:
  static const char *OK;
  static const char *ServerPrefix;

  ThreadConnection **thCon;
  DirectoryConnection **dirCon;
  RemoteConnection *remoteCon;

  ExchangeMeta localMeta; // contain all the rkey and QPN information in this
                          // node, used to exchagne with other nodes

  std::vector<std::string> serverList;

  std::string setKey(uint16_t remoteID) {
    return std::to_string(getMyNodeID()) + "-" + std::to_string(remoteID);
  }

  std::string getKey(uint16_t remoteID) {
    return std::to_string(remoteID) + "-" + std::to_string(getMyNodeID());
  }

  void initLocalMeta();

  void connectMySelf();
  void initRouteRule();

  void setDataToRemote(uint16_t remoteID);
  void setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta);

protected:
  virtual bool connectNode(uint16_t remoteID) override;

public:
  // maxServer is the #servers in our cluster
  DSMKeeper(ThreadConnection **thCon, DirectoryConnection **dirCon,
            RemoteConnection *remoteCon, uint32_t maxServer = 12)
      : Keeper(maxServer), thCon(thCon), dirCon(dirCon), remoteCon(remoteCon) {

    initLocalMeta();

    if (!connectMemcached()) {
      return;
    }
    serverEnter(); // Just tell memcached, I am server 0 or 1 or etc. This is
                   // used to determine the myNodeID in the cluster

    serverConnect();
    connectMySelf();

    initRouteRule();
  }

  ~DSMKeeper() { disconnectMemcached(); }
  void barrier(const std::string &barrierKey);
  void barrier(const std::string &barrierKey, int node_num);
  uint64_t sum(const std::string &sum_key, uint64_t value, bool time_out);
  uint64_t sum(const std::string &sum_key, uint64_t value, int node_num,
               bool time_out);
  uint64_t min(const std::string &max_key, uint64_t value, int node_num);
};

#endif
