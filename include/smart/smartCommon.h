#ifndef __SMART_COMMON_H__
#define __SMART_COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <array>
#include <atomic>
#include <bitset>
#include <limits>
#include <queue>

// Environment Config
#define MAX_CORO_NUM 8
#define ALLOC_ALLIGN_BIT 8

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/coroutine/all.hpp>
#include <boost/crc.hpp>

#define ROUND_UP(x, n) (((x) + (1 << (n)) - 1) & ~((1 << (n)) - 1))
#define ROUND_DOWN(x, n) ((x) & ~((1 << (n)) - 1))

namespace define { // namespace define
// KV
constexpr uint32_t keyLen = 8;
constexpr uint32_t simulatedValLen = 8;
constexpr uint32_t allocAlignLeafSize =
    ROUND_UP(keyLen + simulatedValLen + 16 + 8 + 1, ALLOC_ALLIGN_BIT);

// Internal Node
constexpr uint32_t allocationPageSize = 8 + 8 + 256 * 8;
constexpr uint32_t allocAlignPageSize =
    ROUND_UP(allocationPageSize, ALLOC_ALLIGN_BIT);

// Internal Entry
constexpr uint32_t kvLenBit = 7;
constexpr uint32_t nodeTypeNumBit = 5;
constexpr uint32_t mnIdBit = 8;
constexpr uint32_t offsetBit = 48 - ALLOC_ALLIGN_BIT;
constexpr uint32_t hPartialLenMax = 6;

// constexpr uint64_t kLocalLockNum =
//     4 * MB; // tune to an appropriate value (as small as possible without
//     affect
//             // the performance)

} // namespace define

namespace smart {
using CRCProcessor =
    boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffffffffffff,
                       0xffffffffffffffff, false, false>;

using Key = std::array<uint8_t, define::keyLen>;
using Value = uint64_t;
constexpr uint64_t kKeyMin = 1;
#ifdef KEY_SPACE_LIMIT
constexpr uint64_t kKeyMax = 60000000; // only for int workloads
#endif
constexpr Value kValueNull = std::numeric_limits<Value>::min();
constexpr Value kValueMin = 1;
constexpr Value kValueMax = std::numeric_limits<Value>::max();

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

} // namespace smart

#endif /* __COMMON_H__ */
