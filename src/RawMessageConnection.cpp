#include "RawMessageConnection.h"

#include <cassert>

RawMessageConnection::RawMessageConnection(RdmaContext &ctx, ibv_cq *cq,
                                           uint32_t messageNR)
    : AbstractMessageConnection(IBV_QPT_UD, 0, 40, ctx, cq, messageNR) {}

void RawMessageConnection::initSend() {}

void RawMessageConnection::sendRawMessage(RawMessage *m, uint32_t remoteQPN,
                                          ibv_ah *ah) {
  // I think this is used to dry the queue to avoid the overflow
  // After adding this check, the send queue is at most 32 entries
  // send_cq is different from rpc_cp; rpc_qp is the cp binded to receive queue
  if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0) {
    ibv_wc wc;
    pollWithCQ(send_cq, 1, &wc);
  }

  rdmaSend(message, (uint64_t)m - sendPadding, sizeof(RawMessage) + sendPadding,
           messageLkey, ah, remoteQPN, (sendCounter & SIGNAL_BATCH) == 0);

  ++sendCounter;
}
