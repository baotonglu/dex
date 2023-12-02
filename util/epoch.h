#pragma once

#include "tls_thread.h"
#include "utils.h"
#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <thread>

namespace cachepush {

typedef uint64_t Epoch;

class EpochManager {
public:
  // static uint64_t global_epoch_;

  EpochManager();
  ~EpochManager();

  bool Initialize();
  bool Uninitialize();

  bool Protect() {
    return epoch_table_->Protect(
        current_epoch_.load(std::memory_order_relaxed));
  }

  bool Unprotect() {
    return epoch_table_->Unprotect(
        current_epoch_.load(std::memory_order_relaxed));
  }

  Epoch GetCurrentEpoch() {
    return current_epoch_.load(std::memory_order_seq_cst);
  }

  bool IsSafeToReclaim(Epoch epoch) {
    return epoch <= safe_to_reclaim_epoch_.load(std::memory_order_relaxed);
  }

  Epoch GetReclaimEpoch() {
    return safe_to_reclaim_epoch_.load(std::memory_order_seq_cst);
  }

  bool IsProtected() { return epoch_table_->IsProtected(); }

  void BumpCurrentEpoch();

public:
  void ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

  class MinEpochTable {
  public:
    enum { CACHELINE_SIZE = 64 };

    static const uint64_t kDefaultSize = 128;

    MinEpochTable();
    bool Initialize(uint64_t size = MinEpochTable::kDefaultSize);
    bool Uninitialize();
    bool Protect(Epoch currentEpoch);
    bool Unprotect(Epoch currentEpoch);

    Epoch ComputeNewSafeToReclaimEpoch(Epoch currentEpoch);

    struct Entry {
      /// Construct an Entry in an unlocked and ready to use state.
      Entry() : protected_epoch{0}, last_unprotected_epoch{0}, thread_id{0} {}

      std::atomic<Epoch> protected_epoch; // 8 bytes

      // BT(FIXME): usage of this parameter?
      Epoch last_unprotected_epoch; //  8 bytes

      std::atomic<uint64_t> thread_id; //  8 bytes

      char ___padding[40];

      void *operator new[](uint64_t count) {
        void *mem = nullptr;
        posix_memalign(&mem, CACHELINE_SIZE, count);
        return mem;
      }

      void operator delete[](void *p) { free(p); }

      void *operator new(uint64_t count);

      void operator delete(void *p);
    };
    static_assert(sizeof(Entry) == CACHELINE_SIZE,
                  "Unexpected table entry size");

  public:
    bool GetEntryForThread(Entry **entry);
    Entry *ReserveEntry(uint64_t startIndex, uint64_t threadId);
    Entry *ReserveEntryForThread();
    void ReleaseEntryForThread();
    void ReclaimOldEntries();
    bool IsProtected();

  private:
    Entry *table_;
    uint64_t size_;
  };
  // FIXME(BT): when should the current_epoch be bumped?
  // Is it computed periodically when the garbage size
  // reaches to a threshold? But it is inappliable in
  // my case
  std::atomic<Epoch> current_epoch_;
  // FIXME(BT): usage of this epoch
  // When should following parameter be bumped?
  std::atomic<Epoch> safe_to_reclaim_epoch_;
  MinEpochTable *epoch_table_;

  EpochManager(const EpochManager &) = delete;
  EpochManager(EpochManager &&) = delete;
  EpochManager &operator=(EpochManager &&) = delete;
  EpochManager &operator=(const EpochManager &) = delete;
};

/// Enters an epoch on construction and exits it on destruction. Makes it
/// easy to ensure epoch protection boundaries tightly adhere to stack life
/// time even with complex control flow.
class EpochGuard {
public:
  explicit EpochGuard(EpochManager *epoch_manager)
      : epoch_manager_{epoch_manager}, unprotect_at_exit_(true) {
    epoch_manager_->Protect();
  }

  /// Offer the option of having protext called on \a epoch_manager.
  /// When protect = false this implies "attach" semantics and the caller should
  /// have already called Protect. Behavior is undefined otherwise.
  explicit EpochGuard(EpochManager *epoch_manager, bool protect)
      : epoch_manager_{epoch_manager}, unprotect_at_exit_(protect) {
    if (protect) {
      epoch_manager_->Protect();
    }
  }

  ~EpochGuard() {
    if (unprotect_at_exit_ && epoch_manager_) {
      epoch_manager_->Unprotect();
    }
  }

  /// Release the current epoch manger. It is up to the caller to manually
  /// Unprotect the epoch returned. Unprotect will not be called upon EpochGuard
  /// desruction.
  EpochManager *Release() {
    EpochManager *ret = epoch_manager_;
    epoch_manager_ = nullptr;
    return ret;
  }

private:
  /// The epoch manager responsible for protect/unprotect.
  EpochManager *epoch_manager_;

  /// Whether the guard should call unprotect when going out of scope.
  bool unprotect_at_exit_;
};

EpochManager::EpochManager()
    : current_epoch_{1}, safe_to_reclaim_epoch_{0}, epoch_table_{nullptr} {}

EpochManager::~EpochManager() { Uninitialize(); }

/**
 * Initialize an uninitialized EpochManager. This method must be used before
 * it is safe to use an instance via any other members. Calling this on an
 * initialized instance has no effect.
 *
 * \retval S_OK Initialization was successful and instance is ready for use.
 * \retval S_FALSE This instance was already initialized; no action was taken.
 * \retval E_OUTOFMEMORY Initialization failed due to lack of heap space, the
 *      instance was left safely in an uninitialized state.
 */
bool EpochManager::Initialize() {
  if (epoch_table_)
    return true;

  MinEpochTable *new_table = new MinEpochTable();

  if (new_table == nullptr)
    return false;

  auto rv = new_table->Initialize();
  if (!rv)
    return rv;

  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;
  epoch_table_ = new_table;

  return true;
}

bool EpochManager::Uninitialize() {
  if (!epoch_table_)
    return true;

  auto s = epoch_table_->Uninitialize();

  // Keep going anyway. Even if the inner table fails to completely
  // clean up we want to clean up as much as possible.
  delete epoch_table_;
  epoch_table_ = nullptr;
  current_epoch_ = 1;
  safe_to_reclaim_epoch_ = 0;

  return s;
}

void EpochManager::BumpCurrentEpoch() {
  Epoch newEpoch = current_epoch_.fetch_add(1, std::memory_order_seq_cst);
  ComputeNewSafeToReclaimEpoch(newEpoch);
}

// - private -

void EpochManager::ComputeNewSafeToReclaimEpoch(Epoch currentEpoch) {
  safe_to_reclaim_epoch_.store(
      epoch_table_->ComputeNewSafeToReclaimEpoch(currentEpoch),
      std::memory_order_release);
}

// --- EpochManager::MinEpochTable ---

/// Create an uninitialized table.
EpochManager::MinEpochTable::MinEpochTable() : table_{nullptr}, size_{} {}

bool EpochManager::MinEpochTable::Initialize(uint64_t size) {
  if (table_)
    return true;

  if (!IS_POWER_OF_TWO(size))
    return false;

  Entry *new_table = new Entry[size];
  if (!new_table)
    return false;

  table_ = new_table;
  size_ = size;

  return true;
}

bool EpochManager::MinEpochTable::Uninitialize() {
  if (!table_)
    return true;

  size_ = 0;
  delete[] table_;
  table_ = nullptr;

  return true;
}

/**
 * \param currentEpoch A sequentially consistent snapshot of the current
 *      global epoch. It is okay that this may be stale by the time it
 *      actually gets entered into the table.
 * \return S_OK indicates thread may now enter the protected region. Any
 *      other return indicates a fatal problem accessing the thread local
 *      storage; the thread may not enter the protected region. Most likely
 *      the library has entered some non-serviceable state.
 */
bool EpochManager::MinEpochTable::Protect(Epoch current_epoch) {
  Entry *entry = nullptr;
  if (!GetEntryForThread(&entry)) {
    return false;
  }

  entry->last_unprotected_epoch = 0;
#if 1
  entry->protected_epoch.store(current_epoch, std::memory_order_release);
  // TODO: For this to really make sense according to the spec we
  // need a (relaxed) load on entry->protected_epoch. What we want to
  // ensure is that loads "above" this point in this code don't leak down
  // and access data structures before it is safe.
  // Consistent with http://preshing.com/20130922/acquire-and-release-fences/
  // but less clear whether it is consistent with stdc++.
  std::atomic_thread_fence(std::memory_order_acquire);
#else
  entry->m_protectedEpoch.exchange(currentEpoch, std::memory_order_acq_rel);
#endif
  return true;
}

/**
 * \param currentEpoch A any rough snapshot of the current global epoch, so
 *      long as it is greater than or equal to the value used on the thread's
 *      corresponding call to Protect().
 * \return S_OK indicates thread successfully exited protected region. Any
 *      other return indicates a fatal problem accessing the thread local
 *      storage; the thread may not have successfully exited the protected
 *      region. Most likely the library has entered some non-serviceable
 *      state.
 */
bool EpochManager::MinEpochTable::Unprotect(Epoch currentEpoch) {
  Entry *entry = nullptr;
  if (!GetEntryForThread(&entry)) {
    return false;
  }

  entry->last_unprotected_epoch = currentEpoch;
  std::atomic_thread_fence(std::memory_order_release);
  // 0 means not protected
  entry->protected_epoch.store(0, std::memory_order_relaxed);
  return true;
}

/**
 * \param currentEpoch A snapshot of the current global Epoch; it is okay
 *      that the snapshot may lag the true current epoch slightly.
 * \return An Epoch that can be compared to Epochs associated with items
 *      removed from data structures. If an Epoch associated with a removed
 *      item is less or equal to the returned value, then it is guaranteed
 *      that no future thread will access the item, and it can be reused
 *      (by calling, free() on it, for example). The returned value will
 *      never be equal to or greater than the global epoch at any point, ever.
 *      That ensures that removed items in one Epoch can never be freed
 *      within the same Epoch.
 */
Epoch EpochManager::MinEpochTable::ComputeNewSafeToReclaimEpoch(
    Epoch current_epoch) {
  Epoch oldest_call = current_epoch;
  for (uint64_t i = 0; i < size_; ++i) {
    Entry &entry = table_[i];
    // If any other thread has flushed a protected epoch to the cache
    // hierarchy we're guaranteed to see it even with relaxed access.
    Epoch entryEpoch = entry.protected_epoch.load(std::memory_order_acquire);
    if (entryEpoch != 0 && entryEpoch < oldest_call) {
      oldest_call = entryEpoch;
    }
  }
  // The latest safe epoch is the one just before the earlier unsafe one.
  return oldest_call - 1;
}

// - private -

bool EpochManager::MinEpochTable::GetEntryForThread(Entry **entry) {
  thread_local Entry *tls = nullptr;
  if (tls) {
    *entry = tls;
    return true;
  }

  // No entry index was found in TLS, so we need to reserve a new entry
  // and record its index in TLS
  Entry *reserved = ReserveEntryForThread();
  tls = *entry = reserved;

  Thread::RegisterTls((uint64_t *)&tls, (uint64_t) nullptr);

  return true;
}

uint32_t Murmur3(uint32_t h) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

/**
 * Allocate a new Entry to track a thread's protected/unprotected status and
 * return a pointer to it. This should only be called once for a thread.
 */
EpochManager::MinEpochTable::Entry *
EpochManager::MinEpochTable::ReserveEntryForThread() {
  uint64_t current_thread_id = pthread_self();
  uint64_t startIndex = Murmur3_64(current_thread_id);
  return ReserveEntry(startIndex, current_thread_id);
}

/**
 * Does the heavy lifting of reserveEntryForThread() and is really just
 * split out for easy unit testing. This method relies on the fact that no
 * thread will ever have ID on Windows 0.
 * http://msdn.microsoft.com/en-us/library/windows/desktop/ms686746(v=vs.85).aspx
 */
EpochManager::MinEpochTable::Entry *
EpochManager::MinEpochTable::ReserveEntry(uint64_t start_index,
                                          uint64_t thread_id) {
  for (;;) {
    // Reserve an entry in the table.
    for (uint64_t i = 0; i < size_; ++i) {
      uint64_t indexToTest = (start_index + i) & (size_ - 1);
      Entry &entry = table_[indexToTest];
      if (entry.thread_id == 0) {
        uint64_t expected = 0;
        // Atomically grab a slot. No memory barriers needed.
        // Once the threadId is in place the slot is locked.
        bool success = entry.thread_id.compare_exchange_strong(
            expected, thread_id, std::memory_order_relaxed);
        if (success) {
          return &table_[indexToTest];
        }
        // Ignore the CAS failure since the entry must be populated,
        // just move on to the next entry.
      }
    }
    ReclaimOldEntries();
  }
}

bool EpochManager::MinEpochTable::IsProtected() {
  Entry *entry = nullptr;
#ifdef TEST_BUILD
  auto s = GetEntryForThread(&entry);
  CHECK_EQ(s, true);
#else
  GetEntryForThread(&entry);
#endif
  // It's myself checking my own protected_epoch, safe to use relaxed
  return entry->protected_epoch.load(std::memory_order_relaxed) != 0;
}

void EpochManager::MinEpochTable::ReleaseEntryForThread() {}

void EpochManager::MinEpochTable::ReclaimOldEntries() {}

} // namespace cachepush