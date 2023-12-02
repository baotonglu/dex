#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig {
public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = define::rdmaBufferSize)
      : cacheSize(cacheSize) {}
};

class DSMConfig {
public:
  CacheConfig cacheConfig;
  uint32_t machineNR; // #memory node
  uint32_t computeNR; // #c
  uint64_t dsmSize;   // G
  int memThreadCount;
  int index_type;

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t machineNR = 2, uint64_t dsmSize = define::dsmSize,
            int memThreadCount = 1, int index = 0)
      : cacheConfig(cacheConfig), machineNR(machineNR), dsmSize(dsmSize),
        memThreadCount(memThreadCount), index_type(index) {}
};

#endif /* __CONFIG_H__ */
