#pragma once
#include <atomic>
#include <cstdint>
#include <iostream>
#include <sys/stat.h>
#include <x86intrin.h>

#ifdef TEST_BUILD
#include <glog/logging.h>
#include <glog/raw_logging.h>
#endif

#define IS_POWER_OF_TWO(x) (x && (x & (x - 1)) == 0)

// ADD and SUB return the value after add or sub
#define ADD(_p, _v) (__atomic_add_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define SUB(_p, _v) (__atomic_sub_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define LOAD(_p) (__atomic_load_n(_p, __ATOMIC_SEQ_CST))
#define STORE(_p, _v) (__atomic_store_n(_p, _v, __ATOMIC_SEQ_CST))

#define LOG_FATAL(msg)                                                         \
  std::cout << msg << "\n";                                                    \
  exit(-1)

#define LOG(msg) std::cout << msg << "\n"

inline uint64_t Murmur3_64(uint64_t h) {
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccd;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53;
  h ^= h >> 33;
  return h;
}

template <typename T>
T CompareExchange64(T *destination, T new_value, T comparand) {
  static_assert(sizeof(T) == 8,
                "CompareExchange64 only works on 64 bit values");
  ::__atomic_compare_exchange_n(destination, &comparand, new_value, false,
                                __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return comparand;
}

namespace very_pm {

static const constexpr bool kUseCLWB = true;

static const constexpr uint64_t CREATE_MODE_RW = (S_IWUSR | S_IRUSR);

static const constexpr uint64_t kPMDK_PADDING = 48;

static const constexpr uint64_t kCacheLineSize = 64;

template <typename T>
T CompareExchange64(T *destination, T new_value, T comparand) {
  static_assert(sizeof(T) == 8,
                "CompareExchange64 only works on 64 bit values");
  ::__atomic_compare_exchange_n(destination, &comparand, new_value, false,
                                __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return comparand;
}
} // namespace very_pm
