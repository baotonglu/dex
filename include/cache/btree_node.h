#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <immintrin.h>
#include <iostream>
#include <limits>
#include <list>
#include <sched.h>
#include <unordered_map>
#include <utility>

namespace cachepush {
// #define MINI 1

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };
static const uint64_t swizzle_tag = 1ULL << 63;
static const uint64_t swizzle_hide = (1ULL << 63) - 1;
static const uint64_t pageSize = 1024; // 1KB
static const uint64_t megaLevel =
    4; // 4 level as a Bigger Node to do coarse-grained distribution
// Level 0, 1, ..., MegaLevel -1 are grouped as a sub-tree

#define BITMAP 1

struct OptLock {
  // 0-bit is IO lock; 1-bit is obsolete tag; 2-bit is write-lock;
  // Other bits are for version numbers
  std::atomic<uint64_t> typeVersionLockObsolete{0b1000};

  bool isLocked(uint64_t version) { return ((version & 0b100) == 0b100); }

  bool isLocked() {
    return ((typeVersionLockObsolete.load() & 0b100) == 0b100);
  }

  uint64_t readLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = typeVersionLockObsolete.load();
    if (isLocked(version) || isObsolete(version)) {
      _mm_pause();
      needRestart = true;
    }
    return version;
  }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart =
        ((startRead | 1ULL) != (typeVersionLockObsolete.load() | 1ULL));
  }

  void writeLockOrRestart(bool &needRestart) {
    uint64_t version;
    // version = readLockOrRestart(needRestart);
    version = typeVersionLockObsolete.load();
    if ((version & 0b111) != 0) {
      _mm_pause();
      needRestart = true;
      return;
    }

    upgradeToWriteLockOrRestart(version, needRestart);
    if (needRestart)
      return;
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    if (isIO(version)) {
      needRestart = true;
      return;
    }

    if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                        version + 0b100)) {
      version = version + 0b100;
    } else {
      _mm_pause();
      needRestart = true;
    }
  }

  void IOUpgradeToWriteLockOrRestart(bool &needRestart) {
    uint64_t version;
    // version = readLockOrRestart(needRestart);
    version = typeVersionLockObsolete.load();
    if (!isIO(version)) {
      needRestart = true;
      return;
    }

    typeVersionLockObsolete.store(version - 1 + 0b100);
  }

  void IOLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = typeVersionLockObsolete.load();
    if ((version & 0b111) != 0) {
      _mm_pause();
      needRestart = true;
      return;
    }

    upgradeToIOLockOrRestart(version, needRestart);
    if (needRestart)
      return;
  }

  void upgradeToIOLockOrRestart(uint64_t &version, bool &needRestart) {
    if (isIO(version)) {
      needRestart = true;
      return;
    }

    if (typeVersionLockObsolete.compare_exchange_strong(version, version + 1)) {
      version = version + 1;
    } else {
      _mm_pause();
      needRestart = true;
    }
  }

  void IOUnlock() { typeVersionLockObsolete.fetch_sub(1); }

  void writeUnlock() { typeVersionLockObsolete.fetch_add(0b100); }

  // void writeUnlockWithIOLock() { typeVersionLockObsolete.fetch_add(0b101); }

  void setLockState() { typeVersionLockObsolete.store(0b100); }

  bool isObsolete(uint64_t version) { return (version & 0b10) == 0b10; }

  bool isIO(uint64_t version) { return (version & 1) == 1; }

  void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b110); }
};

// 64B - one cacheline size
struct NodeBase : OptLock {
  uint64_t front_version;
  GlobalAddress remote_address;
  uint64_t bitmap;      // 32B
  NodeBase *parent_ptr; // The ptr to the parent;
  PageType type;
  uint8_t count;
  uint8_t level;     // 0 means leaf, 1 means inner node untop of leaf nodes
  uint8_t pos_state; // 0 means in remote memory, 1 means in cooling state, 2
                     // means in hot state, 3 means in local working set
                     // (e.g., after delete or evict) 40B
                     // 4 means pinnning => cannot be sampled...
  bool obsolete;     // obsolete means this page is obsolete
  bool shared;
  bool is_smo; // no use now
  bool dirty;
  // Actually level and type could be merged
  // used for concurrent sync in node accesses
  // (min_limit_, max_limit_]
  Key min_limit_;
  Key max_limit_; // 64B

  bool check_obsolete() { return obsolete; }

  bool isShared() { return shared == true; }
};

struct BTreeLeafBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeLeaf;
};

template <class Key, class Payload> struct BTreeLeaf : public BTreeLeafBase {
  // This is the element type of the leaf node
  using KeyValueType = std::pair<Key, Payload>;
  static const uint64_t maxEntries =
      (pageSize - (sizeof(Key) * 2 + sizeof(uint64_t) * 2) - sizeof(NodeBase)) /
      (sizeof(KeyValueType));

  // This is the array that we perform search on
  KeyValueType data[maxEntries];

  // BTreeLeaf<Key, Payload> *next_leaf=nullptr;
  GlobalAddress next_leaf = GlobalAddress::Null();

  uint64_t back_version;

  uint64_t dummy[2]; // pack for 1024 granularity

  BTreeLeaf(GlobalAddress remote) {
    remote_address = remote;
    parent_ptr = nullptr;
    level = 0;
    count = 0;
    pos_state = 0;
    dirty = true;
    obsolete = false;
    shared = false;
    type = typeMarker;
    // lock = 0;
    front_version = 0;
    back_version = 0;
    bitmap = 0;
    min_limit_ = std::numeric_limits<Key>::min();
    max_limit_ = std::numeric_limits<Key>::max();
    is_smo = false;
  }

  bool isFull() { return count == maxEntries; }

  bool check_consistent() { return front_version == back_version; }

  unsigned lowerBound(Key k) {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      // This is the key at the pivot position
      const Key &middle_key = data[mid].first;

      if (k < middle_key) {
        upper = mid;
      } else if (k > middle_key) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  bool find(Key k, Value &v) {
    auto pos = lowerBound(k);
    bool success = false;
    if ((pos < count) && data[pos].first == k) {
      success = true;
      v = data[pos].second;
    }
    return success;
  }

  uint32_t range_scan(Key k, uint32_t num, KeyValueType *&array) {
    auto pos = lowerBound(k);
    if ((pos < count) && data[pos].first == k) {
      uint32_t remain_count = count - pos;
      uint32_t copy_count = std::min<uint32_t>(remain_count, num);
      memcpy(array, data + pos, copy_count * sizeof(KeyValueType));
      return copy_count;
    }
    return 0;
  }

  bool insert(Key k, Payload p) {
    // assert(count < maxEntries);
    dirty = true;
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // Upsert
        data[pos].second = p;
        front_version++;
        back_version++;
        return false;
      }

      if (count >= maxEntries)
        return false;

      memmove(reinterpret_cast<void *>(data + pos + 1),
              reinterpret_cast<void *>(data + pos),
              sizeof(KeyValueType) * (count - pos));
      // memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
      data[pos].first = k;
      data[pos].second = p;
    } else {
      data[0].first = k;
      data[0].second = p;
    }

    count++;
    front_version++;
    back_version++;
    return true;
  }

  bool remove(Key k) {
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        dirty = true;
        // move data elements
        memmove(reinterpret_cast<void *>(data + pos),
                reinterpret_cast<void *>(data + pos + 1),
                sizeof(KeyValueType) * (count - pos - 1));
        --count;
        front_version++;
        back_version++;
        return true;
      }
    }
    return false;
  }

  bool update(Key k, Payload p) {
    auto pos = lowerBound(k);
    bool success = false;
    if ((pos < count) && data[pos].first == k) {
      front_version++;
      dirty = true;
      success = true;
      data[pos].second = p;
      back_version++;
    }
    return success;
  }

  // No use
  void populate(std::pair<Key, Value> &array, int begin, int end) {
    count = end - begin;
    assert(count <= maxEntries);
    for (int i = 0; i < count; ++i) {
      data[i].first = array[begin + i].first;
      data[i].second = array[begin + i].second;
    }
    dirty = true;
  }

  void split(Key &sep, BTreeLeaf *newLeaf, GlobalAddress remote_node) {
    front_version++;
    is_smo = true;
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(reinterpret_cast<void *>(newLeaf->data),
           reinterpret_cast<void *>(data + count),
           sizeof(KeyValueType) * newLeaf->count);
    this->next_leaf = remote_node;
    sep = data[count - 1].first;

    // Reset the range info for the leaf nodes
    newLeaf->min_limit_ = sep;
    newLeaf->max_limit_ = max_limit_;
    max_limit_ = sep;

    dirty = true;
    back_version++;
  }

  bool rangeValid(Key k) {
    if (std::numeric_limits<Key>::min() == k && k == min_limit_)
      return true;
    if (k <= min_limit_ || k > max_limit_)
      return false;
    return true;
  }
};

struct BTreeInnerBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeInner;
};

template <class Key> struct BTreeInner : public BTreeInnerBase {
  static const uint64_t maxEntries =
      (pageSize - sizeof(uint64_t) - sizeof(NodeBase)) /
      (sizeof(Key) + sizeof(GlobalAddress));
  GlobalAddress children[maxEntries];
  Key keys[maxEntries];
  uint64_t back_version;
  uint64_t dummy;

  BTreeInner(uint8_t cur_level, GlobalAddress remote) {
    level = cur_level;
    remote_address = remote;
    parent_ptr = nullptr;
    count = 0;
    obsolete = false;
    pos_state = 0;
    shared = false;
    dirty = true;
    type = typeMarker;
    bitmap = 0;
    // lock = 0;
    front_version = 0;
    back_version = 0;
    min_limit_ = std::numeric_limits<Key>::min();
    max_limit_ = std::numeric_limits<Key>::max();
    is_smo = false;
  }

  bool isFull() { return count == (maxEntries - 1); }

  bool check_consistent() { return front_version == back_version; }

  unsigned lowerBound(Key k) const {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (k < keys[mid]) {
        upper = mid;
      } else if (k > keys[mid]) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  int findIdx(uint64_t target_addr) {
    int end = count + 1;
    for (int i = 0; i < end; i++) {
      if (children[i].val == target_addr) {
        return i;
      }
    }
    return -1;
  }

  void set_bitmap(int idx) { bitmap = bitmap | (1ULL << idx); }

  void unset_bitmap(int idx) { bitmap = bitmap & (~(1ULL << idx)); }

  bool test_bimap(int idx) { return bitmap & (1ULL << idx); }

  // Returns position of closest 1 to pos
  // Returns pos if pos is a set
  int closest_set(int pos) const {
    if (bitmap == 0)
      return -1;

    int bit_pos = std::min(pos, 63);
    uint64_t bitmap_data = bitmap;
    int closest_right_gap_distance = 64;
    int closest_left_gap_distance = 64;
    // Logically sets to the right of pos, in the bitmap these are sets to the
    // left of pos's bit
    // This covers the case where pos is a 1
    // cover idx: [pos, 63]
    uint64_t bitmap_right_sets = bitmap_data & (~((1ULL << bit_pos) - 1));
    if (bitmap_right_sets != 0) {
      closest_right_gap_distance =
          static_cast<int>(_tzcnt_u64(bitmap_right_sets)) - bit_pos;
    }

    // Logically sets to the left of pos, in the bitmap these are sets to the
    // right of pos's bit
    // cover idx: [0, pos - 1]
    uint64_t bitmap_left_sets = bitmap_data & ((1ULL << bit_pos) - 1);
    if (bitmap_left_sets != 0) {
      closest_left_gap_distance =
          bit_pos - (63 - static_cast<int>(_lzcnt_u64(bitmap_left_sets)));
    }

    if (closest_right_gap_distance < closest_left_gap_distance &&
        pos + closest_right_gap_distance < (count + 1)) {
      return pos + closest_right_gap_distance;
    } else {
      return pos - closest_left_gap_distance;
    }
  }

  void split(Key &sep, BTreeInner *newInner) {
    front_version++;
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1,
           sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1,
           sizeof(GlobalAddress) * (newInner->count + 1));
    newInner->min_limit_ = sep;
    newInner->max_limit_ = max_limit_;
    max_limit_ = sep;
    newInner->bitmap = bitmap >> (count + 1);
    bitmap = bitmap & ((1ULL << (count + 1)) - 1);
    dirty = true;
    is_smo = true;
    back_version++;
  }

  bool rangeValid(Key k) {
    if (std::numeric_limits<Key>::min() == k && k == min_limit_)
      return true;
    if (k <= min_limit_ || k > max_limit_)
      return false;
    return true;
  }

  void insert(Key k, GlobalAddress child) {
    assert(count < maxEntries - 1);
    front_version++;
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos,
            sizeof(GlobalAddress) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);

    uint64_t upper_part = ((bitmap >> (pos + 1)) << (pos + 2));
    bitmap = (bitmap & ((1ULL << (pos + 1)) - 1)) | upper_part;
    if (child.val & swizzle_tag) {
      set_bitmap(pos + 1);
    }
    count++;
    dirty = true;
    back_version++;
  }

  void validate_bitmap_correctness(int tag) {
    for (int i = 0; i < count + 1; ++i) {
      if (children[i].val & swizzle_tag) {
        if (!test_bimap(i)) {
          std::cout << "Tag = " << tag << std::endl;
          std::cout << "Tree level = " << static_cast<int>(level) << std::endl;
          std::cout << "Swizzling but bitmap not set at idx " << i << std::endl;
          exit(-1);
        }
      } else {
        if (test_bimap(i)) {
          std::cout << "2 Tag = " << tag << std::endl;
          std::cout << "2 Tree level = " << static_cast<int>(level)
                    << std::endl;
          std::cout << "2 Swizzling but bitmap not set at idx " << i
                    << std::endl;
          exit(-1);
        }
      }
    }
  }
};

} // namespace cachepush