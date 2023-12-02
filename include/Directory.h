#ifndef __DIRECTORY_H__
#define __DIRECTORY_H__

#include <thread>

#include <unordered_map>

#include "Common.h"

#include "Connection.h"
#include "GlobalAllocator.h"

class Directory {
public:
  Directory(DirectoryConnection *dCon, RemoteConnection *remoteInfo,
            uint32_t machineNR, uint16_t dirID, uint16_t nodeID,
            int memThreadCount);

  ~Directory();

private:
  DirectoryConnection *dCon;
  RemoteConnection *remoteInfo;

  uint32_t machineNR;
  uint16_t dirID;
  uint16_t nodeID;

  std::thread *dirTh;

  GlobalAllocator *chunckAlloc;

  void dirThread();

  // No sepecific implementation, "process_message" below has the
  // funcitonability to handle the message and reply
  void sendData2App(const RawMessage *m);

  void process_message(const RawMessage *m);
};

#endif /* __DIRECTORY_H__ */
