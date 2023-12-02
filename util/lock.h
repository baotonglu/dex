#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>

// Simple optimistic CAS locks

#define CAS(_p, _u, _v)                                                        \
  (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,            \
                               __ATOMIC_ACQUIRE))

namespace cachepush {

const uint64_t lockSet = ((uint64_t)1ULL << 63);
const uint64_t lockMask = ((uint64_t)1ULL << 63) - 1;
const uint32_t lockSet32 = ((uint32_t)1U << 31);
const uint32_t lockMask32 = ((uint32_t)1U << 31) - 1;

class Lock {
public:
  uint64_t version_lock = 0;
  Lock() { version_lock = 0; }

  void reset() { version_lock = 0; }

  void get_lock() {
    uint64_t new_value = 0;
    uint64_t old_value = 0;
    do {
      while (true) {
        old_value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
        if (!(old_value & lockSet)) {
          old_value &= lockMask;
          break;
        }
      }
      new_value = old_value | lockSet;
    } while (!CAS(&version_lock, &old_value, new_value));
  }

  bool try_get_lock() {
    uint64_t v = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    if (v & lockSet) {
      return false;
    }
    auto old_value = v & lockMask;
    auto new_value = v | lockSet;
    return CAS(&version_lock, &old_value, new_value);
  }

  void release_lock() {
    uint64_t v = version_lock;
    __atomic_store_n(&version_lock, v + 1 - lockSet, __ATOMIC_RELEASE);
  }

  /*if the lock is set, return true*/
  bool test_lock_set(uint64_t &version) {
    version = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    return (version & lockSet) != 0;
  }

  // test whether the version has change, if change, return true
  bool test_lock_version_change(uint64_t old_version) {
    auto value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    return (old_version != value);
  }
};

class Lock32 {
public:
  uint32_t version_lock = 0;
  Lock32() { version_lock = 0; }

  void reset() { version_lock = 0; }

  void get_lock() {
    uint32_t new_value = 0;
    uint32_t old_value = 0;
    do {
      while (true) {
        old_value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
        if (!(old_value & lockSet32)) {
          old_value &= lockMask32;
          break;
        }
      }
      new_value = old_value | lockSet32;
    } while (!CAS(&version_lock, &old_value, new_value));
  }

  bool try_get_lock() {
    uint32_t v = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    if (v & lockSet32) {
      return false;
    }
    auto old_value = v & lockMask32;
    auto new_value = v | lockSet32;
    return CAS(&version_lock, &old_value, new_value);
  }

  void release_lock() {
    uint32_t v = version_lock;
    __atomic_store_n(&version_lock, v + 1 - lockSet32, __ATOMIC_RELEASE);
  }

  /*if the lock is set, return true*/
  bool test_lock_set(uint32_t &version) {
    version = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    return (version & lockSet32) != 0;
  }

  // test whether the version has change, if change, return true
  bool test_lock_version_change(uint32_t old_version) {
    auto value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    return (old_version != value);
  }

  bool upgrade_to_exclusive_lock(uint32_t old_version) {
    uint64_t cur_value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
    if (cur_value != old_version)
      return false;
    uint64_t new_value = cur_value | lockSet;
    return CAS(&version_lock, &cur_value, new_value);
  }
};

} // namespace cachepush