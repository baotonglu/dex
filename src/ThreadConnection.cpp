#include "ThreadConnection.h"

#include "Connection.h"
// Create the QP endpoint for one thread?
ThreadConnection::ThreadConnection(uint16_t threadID, void *cachePool,
                                   uint64_t cacheSize, uint32_t machineNR,
                                   RemoteConnection *remoteInfo)
    : threadID(threadID), remoteInfo(remoteInfo) {
  auto create_flag = createContext(&ctx);
  assert(create_flag == true);

  cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
  // rpc_cq = cq;
  rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);

  message = new RawMessageConnection(ctx, rpc_cq, APP_MESSAGE_NR);

  this->cachePool = cachePool;
  cacheMR = createMemoryRegion((uint64_t)cachePool, cacheSize, &ctx);
  cacheLKey = cacheMR->lkey;

  // dir, RC
  // for (int i = 0; i < NR_DIRECTORY; ++i) {
  for (int i = 0; i < 1; ++i) {
    data[i] = new ibv_qp *[machineNR];
    for (size_t k = 0; k < machineNR; ++k) {
      createQueuePair(&data[i][k], IBV_QPT_RC, cq, &ctx);
    }
  }
}

void ThreadConnection::sendMessage2Dir(RawMessage *m, uint16_t node_id,
                                       uint16_t dir_id) {
  // printf("Send a message to node %d and dir %d, qpn = %d\n", node_id, dir_id,
  //        remoteInfo[node_id].dirMessageQPN[dir_id]);
  message->sendRawMessage(m, remoteInfo[node_id].dirMessageQPN[dir_id],
                          remoteInfo[node_id].appToDirAh[threadID][dir_id]);
  // printf("Finish the message send\n");
}
