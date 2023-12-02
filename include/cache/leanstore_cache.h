#pragma once

/* This cache management is specialized for B+-Tree */
/* Here I try to implement the idea of LeanStore and seek opportunies of further
 * optimization*/
#include <atomic>
#include <list>
#include <random>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "../../util/cache_allocator.h"
#include "../../util/epoch.h"
#include "../../util/lock.h"
#include "../tree/hash_table.h"
#include "btree_node.h"
#include "latency_collector.h"
#include "node_wr.h"
#include <inttypes.h>

namespace cachepush {
// FIXME(BT): these two variable is better to be included in the cache
// containeer
thread_local static std::vector<void *> local_page_set;
thread_local static LatencyCollector decision;
// FIXME(BT): need a sensitive study for the following parameters
int probing_length = 8;
int num_pages_to_sample = 2;
#define CONCURRENT 1
uint64_t sample_num = 0;
uint64_t total_sample_loop = 0;
uint64_t wrong_page_state = 0;
uint64_t wrong_iteration = 0;
uint64_t wrong_parent = 0;
uint64_t wrong_parent_lock = 0;
uint64_t wrong_parent_check = 0;
uint64_t wrong_evict = 0;
uint64_t one_state = 0;
uint64_t zero_state = 0;
uint64_t three_state = 0;

enum class RPC_type { LOOKUP, UPDATE, INSERT, DELETE };

#define LATENCY_COLLECT 1

class CacheManager {
public:
  /* Core Data Structure */
  uint64_t capacity;      // Number of pages used in the cache of CPU node
  std::atomic<int> state; // 0 means warm-up phase, also use the page from
                          // allocator; 1 means dynamic phase

  /* Below is cache statistic */
  uint64_t inner_miss_ = 0;     // inner read miss
  uint64_t leaf_miss_ = 0;      // leaf read miss
  uint64_t full_page_miss_ = 0; // full read miss
  uint64_t rdma_write = 0;

  // Concurrent hash table
  hash_table *hash_table_;

  // Concurrent page table
  page_table *page_table_;

  // Limit information
  Key buffer_min_limit_;
  Key buffer_max_limit_;

  GlobalAddress *root_ptr_;

  double rpc_rate_; // the ratio for pushdown
  double admission_rate_;

  // static thread_local LatencyCollector decision;
  //   static thread_local LatencyCollector decision(100);

  CacheManager(uint64_t cache_capacity, double cooling_ratio, double rpc_rate,
               double admission_rate, GlobalAddress *root_ptr = nullptr) {
    rpc_rate_ = std::max<double>(0, std::min<double>(rpc_rate, 0.99));
    std::cout << "Pushdown (RPC) Rate: " << rpc_rate_ << std::endl;
    admission_rate_ = admission_rate;
    std::cout << "Admission Rate: " << admission_rate_ << std::endl;

    root_ptr_ = root_ptr;
    capacity = cache_capacity;
    state = 0;
    cache_allocator::initialize(pageSize, cache_capacity * pageSize);
#ifdef PAGE_TABLE
    page_table_ = new page_table(cache_capacity);
    hash_table_ = new hash_table(cache_capacity * cooling_ratio, page_table_);
#else
    hash_table_ = new hash_table(cache_capacity * cooling_ratio);
#endif
  }

  double get_rpc_ratio() {
    // return decision.pushdown_rate();
    return 0;
  }

  void set_rpc_ratio(double ratio) { rpc_rate_ = ratio; }
  void set_admission_ratio(double ratio) { admission_rate_ = ratio; }

  /*----------------------------------*/
  /***** Cache management *****/
  /*----------------------------------*/
  NodeBase *search_in_cache(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      return reinterpret_cast<NodeBase *>(node & swizzle_hide);
    }
    return nullptr;
  }

  bool is_in_cache(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      return true;
    }
    return false;
  }

  // The compute node side already gets the memory block through one-sided read
  // Now it decides to insert this memory block into the cache
  // it does not replace outdated cache element (if it exists in the cache)
  NodeBase *cache_insert(GlobalAddress &global_node, NodeBase *cache_node,
                         NodeBase *parent_ptr, bool add_to_pt = true) {
    // cache_allocator::Unprotect();
    void *page = get_empty_page();
    // memcpy(page, cache_node, pageSize);
    memcpy(reinterpret_cast<char *>(page) + 8,
           reinterpret_cast<char *>(cache_node) + 8, pageSize - 8);

    auto return_page = reinterpret_cast<NodeBase *>(page);
    return_page->pos_state = 4; // means this page can not be sampled
    return_page->parent_ptr = parent_ptr;
    GlobalAddress snapshot = global_node;
    // FIXME(BT): no need to setup bitmap?
    global_node.val = reinterpret_cast<uint64_t>(page) | swizzle_tag;
    assert(return_page->isLocked());

#ifdef PAGE_TABLE
    if (add_to_pt) {
      // insert return page to page_table_
      page_table_->upsert_with_lock(snapshot, page);
    }
#endif

    // cache_allocator::Protect();
    return return_page;
  }

  // HOT => COOLING (...=> COLD)
  void sample_page() {
    int counter = 0;
    bool verbose = false;
    while (true) {
      ++counter;
      if (counter >= 900) {
        verbose = true;
      }

      if (counter >= 1000) {
        exit(0);
        break;
      }
      // if (counter > 64)
      //   break;
      auto page =
          reinterpret_cast<NodeBase *>(cache_allocator::random_select());
      if (page->pos_state != 2)
        continue;
      int idx_in_parent = -1;
      auto cur_page = recursive_iterate(page, idx_in_parent);
      if (cur_page == nullptr)
        continue;
      auto parent = reinterpret_cast<BTreeInner<Key> *>(cur_page->parent_ptr);
      if (parent && parent->level != 255) {
        assert(cur_page->pos_state == 2);
        bool needRestart = false;
        parent->writeLockOrRestart(needRestart);
        if (needRestart) {
          cur_page->writeUnlock();
          continue;
        }
        // assert(cur_page->parent_ptr == parent);
        if (cur_page->parent_ptr != parent) {
          parent->writeUnlock();
          cur_page->writeUnlock();
          continue;
        }

        if (idx_in_parent == -1) {
          idx_in_parent = parent->findIdx(reinterpret_cast<uint64_t>(cur_page) |
                                          swizzle_tag);
        }

        if (idx_in_parent == -1) {
          std::cout << "The parent's idx is wrong!!!" << std::endl;
          check_parent_child_info(parent, cur_page);
          while (true)
            ;
        }

        assert(idx_in_parent != -1);
        assert(check_parent_child_info(parent, cur_page));

        // Unswizzles
        parent->children[idx_in_parent].val = cur_page->remote_address.val;
        parent->unset_bitmap(idx_in_parent);
        cur_page->parent_ptr = nullptr;
        NodeBase *evict_page = nullptr;
        if (cur_page->dirty) {
          remote_write(cur_page->remote_address, cur_page, true);
        }
        hash_table_->insert(cur_page->remote_address.val,
                            reinterpret_cast<void *>(cur_page),
                            reinterpret_cast<void **>(&evict_page));
        if (evict_page != nullptr) {
          insert_local_set(reinterpret_cast<uint64_t>(evict_page));
        }
        parent->writeUnlock();
        return;
        // cache_allocator::BumpCurrentEpoch();
      } else if (parent == nullptr) {
        assert(cur_page->parent_ptr == nullptr);
        if (cur_page->dirty) {
          remote_write(cur_page->remote_address, cur_page, true);
        }
        NodeBase *evict_page = nullptr;
        hash_table_->insert(cur_page->remote_address.val,
                            reinterpret_cast<void *>(cur_page),
                            reinterpret_cast<void **>(&evict_page));
        if (evict_page != nullptr) {
          insert_local_set(reinterpret_cast<uint64_t>(evict_page));
        }
        return;
      }
      cur_page->writeUnlock();
    }
  }

  // HOT => COOLING
  void sample_multiple_pages(int count) {
    while (count) {
      sample_page(); // Need to write back the dirty data??
      --count;
    }
  }

  /*----------------------------------*/
  /***** Mini/Full Page management *****/
  /*----------------------------------*/
  void insert_local_set(uint64_t addr) {
    auto node = reinterpret_cast<NodeBase *>(addr);
    assert(node->isLocked());
    node->pos_state = 3;
    local_page_set.push_back(reinterpret_cast<void *>(addr));
  }

  void *get_local_page_set() {
    if (local_page_set.empty()) {
      return nullptr;
    }
    auto ret = local_page_set.back();
    local_page_set.pop_back();
    return ret;
  }

  // Strategy to get the empty page from buffer pool
  void *try_get_empty_page() {
    void *page = nullptr;
    while (true) {
      if (state == 1) {
        if (!local_page_set.empty()) {
          page = get_local_page_set();
          break;
        } else {
          // Evict from the cooling table
          int ret = hash_table_->random_evict_to_remote(&page, probing_length);
          if (ret == -1) {
            // cache_allocator::BumpCurrentEpoch();
            sample_multiple_pages(num_pages_to_sample);
          }

          if (page == nullptr && (!local_page_set.empty())) {
            page = get_local_page_set();
            assert(page != nullptr);
          }
          break;
        }
      } else { // state == 0
        bool last_page_flag = false;
        page = cache_allocator::allocate(last_page_flag);
        // If this is the last page,
        // we need to increment the state of the buffer pool
        if (last_page_flag) {
          std::cout << "entering dynamic phase" << std::endl;
          state.store(1);
        }

        if (page != nullptr) {
          reinterpret_cast<NodeBase *>(page)->setLockState();
          break;
        }
      }
    }

    if (page != nullptr) {
      assert(reinterpret_cast<NodeBase *>(page)->isLocked());
    }

    return page;
  }

  void *get_empty_page() {
    void *page = nullptr;
    while (true) {
      page = try_get_empty_page();
      if (page != nullptr)
        break;
    }
    return page;
  }

  void swizzling(GlobalAddress &global_addr, NodeBase *parent,
                 unsigned child_idx, NodeBase *child) {
    if (parent) {
      if (!check_parent_child_info(parent, child)) {
        std::cout << "Gloobal addr = " << global_addr << std::endl;
        auto new_child = raw_remote_read(child->remote_address);
        assert(check_parent_child_info(parent, child));
      }
    }

    child->parent_ptr = parent;
    child->pos_state = 2;
    global_addr.val = reinterpret_cast<uint64_t>(child) | swizzle_tag;
    if (parent) {
      auto inner_parent = reinterpret_cast<BTreeInner<Key> *>(parent);
      inner_parent->set_bitmap(child_idx);
      if (inner_parent->level == 255) {
        inner_parent->children[0].val =
            reinterpret_cast<uint64_t>(child) | swizzle_tag;
      }
      assert(check_parent_child_info(parent, child));
    }
  }

  void unswizzling(GlobalAddress &global_addr, NodeBase *parent,
                   unsigned child_idx, NodeBase *child) {
    auto inner_parent = reinterpret_cast<BTreeInner<Key> *>(parent);
    inner_parent->unset_bitmap(child_idx);
    global_addr.val = child->remote_address.val;
    child->parent_ptr = nullptr;
  }

  bool fit_limits(NodeBase *cur_node) {
    if ((cur_node->min_limit_ >= buffer_max_limit_) ||
        (cur_node->max_limit_ < buffer_min_limit_))
      return false;
    return true;
  }

  // To test whether the loaded node belongs to this computing node
  bool sync_or_not(BTreeInner<Key> *inner, uint64_t idx) {
    if (inner->level == 255)
      return true;
    Key min_limit = (idx == 0) ? inner->min_limit_ : inner->keys[idx - 1];
    Key max_limit =
        (idx == inner->count) ? inner->max_limit_ : inner->keys[idx];
    if (min_limit >= buffer_min_limit_ && max_limit < buffer_max_limit_) {
      return false;
    }
    return true;
  }

  void opportunistic_sample() {
    if (state == 1 && local_page_set.empty()) {
      // Start sampling: hot to cooling
      sample_multiple_pages(num_pages_to_sample);
    }
  }

  inline void fill_local_page_set() {
    if (local_page_set.empty()) {
      auto new_page = get_empty_page();
      insert_local_set(reinterpret_cast<uint64_t>(new_page));
    }
  }

  // It relies on the pointer swizzling information
  NodeBase *cache_get(GlobalAddress node, NodeBase *parent, unsigned child_idx,
                      bool &restart, bool &refresh, bool IO_enable) {
    // Start the search in page table
    auto head_page_bucket = page_table_->get_bucket(node);
    // head_page_bucket->lock_.get_lock();
    bool lock_success = head_page_bucket->lock_.try_get_lock();
    if (!lock_success) {
      restart = true;
      return nullptr;
    }
    auto target_page = page_table_->get(head_page_bucket, node);
    // 1.0 Cold => Hot
    if (target_page == nullptr) {
      // Add IO flag
      page_table_->insert_io_flag(head_page_bucket, node);
      head_page_bucket->lock_.release_lock();
      if (!IO_enable)
        return nullptr;

      restart = true;
      NodeBase *return_page = nullptr;
      cold_to_hot(node, reinterpret_cast<void **>(&return_page), parent,
                  child_idx, refresh);
      // bool sync_read =
      //     sync_or_not(reinterpret_cast<BTreeInner<Key> *>(parent),
      //     child_idx);
      // NodeBase *return_page = nullptr;
      // auto ret =
      //     simple_cold_to_hot(node, reinterpret_cast<void **>(&return_page),
      //                        parent, child_idx, refresh, sync_read);
      // // Remove IO flag
      // if (ret == 0) {
      //   void *old_flag = nullptr;
      //   auto flag = page_table_->update_with_lock(
      //       node, reinterpret_cast<void *>(return_page), &old_flag);
      //   assert(flag == true);
      //   assert(reinterpret_cast<uint64_t>(old_flag) == IO_FLAG);
      // } else {
      //   auto flag = page_table_->remove_with_lock(
      //       node, reinterpret_cast<void *>(IO_FLAG));
      //   assert(flag == true);
      // }
      return return_page;
    } else if (reinterpret_cast<uint64_t>(target_page) == IO_FLAG) {
      restart = true;
      head_page_bucket->lock_.release_lock();
      return nullptr;
    }

    // 2.0 Cool => hot or Hot => hot
    // Get the exclusive lock of this node and then get the exclusive lock
    // of this parent
    auto target_node = reinterpret_cast<NodeBase *>(target_page);
    bool exclusive_success =
        get_exclusive_node(GlobalAddress(node), target_node);
    if (!exclusive_success) {
      restart = true;
      head_page_bucket->lock_.release_lock();
      return nullptr;
    }
    assert(target_node->isLocked());

    // Using range to check
    if (!new_check_limit_match(parent, target_node, child_idx)) {
      assert(parent->isShared() || target_node->isShared());
      if (target_node->isShared()) {
        // Check whether it is outdated
        auto remote_target_node = reinterpret_cast<BTreeInner<Key> *>(
            raw_remote_read(target_node->remote_address));
        check_global_conflict(remote_target_node, target_node->front_version,
                              restart);
        if (restart) {
          target_node->obsolete = true;
          // remove the outdated page from the page table
          auto flag = page_table_->remove(head_page_bucket, node, target_page);
          assert(flag == true);
          head_page_bucket->lock_.release_lock();
          target_node->pos_state = 2;
          target_node->writeUnlock();
          refresh = true;
          return nullptr;
        }
      }

      if (parent->isShared()) {
        auto remote_parent_node = reinterpret_cast<BTreeInner<Key> *>(
            raw_remote_read(parent->remote_address));
        check_global_conflict(remote_parent_node, parent->front_version,
                              restart);
        if (restart) {
          // std::cout << "Refresh because parent is not obsolete" << std::endl;
          head_page_bucket->lock_.release_lock();
          target_node->pos_state = 2;
          target_node->writeUnlock();
          refresh = true;
          return nullptr;
        }
      }
    }

    // Do the swizzling
    head_page_bucket->lock_.release_lock();
    return target_node;
  }

  // -1 means failure and retry
  // 0 means cold to hot succeeds
  // 1 means one-sided update succeeds
  int cold_to_hot_with_admission(GlobalAddress global_node, void **ret_page,
                                 NodeBase *parent, unsigned child_idx,
                                 bool &refresh, Key k, Value &result,
                                 bool &success, RPC_type rpc_type) {
    static thread_local std::mt19937 *generator = nullptr;
    if (!generator)
      generator = new std::mt19937(clock() + pthread_self());
    static thread_local std::uniform_int_distribution<uint64_t> distribution(
        0, 9999);
    auto idx = distribution(*generator);
    uint64_t admission_idx = 10000 * admission_rate_;
    if (state == 1 && idx >= admission_idx) {
      // Just read from remote and return to the application
      int ret = 1;
      switch (rpc_type) {
      case RPC_type::LOOKUP: {
        auto buffer_page = raw_remote_read(global_node);
        auto cur_leaf = reinterpret_cast<BTreeLeaf<Key, Value> *>(buffer_page);
        if (!cur_leaf->rangeValid(k)) {
          ret = -1;
        } else {
          success = cur_leaf->find(k, result);
        }
        break;
      }

      case RPC_type::UPDATE: {
        auto buffer_page = raw_remote_read(global_node);
        auto cur_leaf = reinterpret_cast<BTreeLeaf<Key, Value> *>(buffer_page);
        if (!cur_leaf->rangeValid(k)) {
          ret = -1;
        } else {
          success = cur_leaf->update(k, result);
          if (success)
            remote_write(global_node, buffer_page, true, true);
        }
        break;
      }

      case RPC_type::INSERT: {
        auto buffer_page = raw_remote_read(global_node);
        auto cur_leaf = reinterpret_cast<BTreeLeaf<Key, Value> *>(buffer_page);
        if ((!cur_leaf->rangeValid(k)) ||
            (cur_leaf->count == cur_leaf->maxEntries)) {
          ret = -1;
        } else {
          success = cur_leaf->insert(k, result);
          remote_write(global_node, buffer_page, true, true);
        }
        break;
      }

      case RPC_type::DELETE: {
        auto buffer_page = raw_remote_read(global_node);
        auto cur_leaf = reinterpret_cast<BTreeLeaf<Key, Value> *>(buffer_page);
        if (!cur_leaf->rangeValid(k)) {
          ret = -1;
        } else {
          success = cur_leaf->remove(k);
          if (success)
            remote_write(global_node, buffer_page, true, true);
        }
        break;
      }

      default:
        ret = -1;
        break;
      }
      bool flag = page_table_->remove_with_lock(
          global_node, reinterpret_cast<void *>(IO_FLAG));
      assert(flag == true);
      return ret;
    }

    return cold_to_hot(global_node, ret_page, parent, child_idx, refresh);
    // No sync read is needed because leaf nodes are exclusive
    // auto ret = simple_cold_to_hot(global_node, ret_page, parent, child_idx,
    //                               refresh, false);
    // if (ret == 0) {
    //   void *old_flag = nullptr;
    //   auto flag = page_table_->update_with_lock(
    //       global_node, reinterpret_cast<void *>(*ret_page), &old_flag);
    //   assert(flag == true);
    //   assert(reinterpret_cast<uint64_t>(old_flag) == IO_FLAG);
    // } else {
    //   auto flag = page_table_->remove_with_lock(
    //       global_node, reinterpret_cast<void *>(IO_FLAG));
    //   assert(flag == true);
    // }
    // return ret;
  }

  // -1 means failure and retry
  // 0 means cold to hot succeeds
  // 1 means one-sided update succeeds
  int cold_to_hot_with_admission_for_scan(GlobalAddress global_node,
                                          void **ret_page, NodeBase *parent,
                                          unsigned child_idx, bool &refresh,
                                          Key k,
                                          std::pair<Key, Value> *&kv_buffer,
                                          int &scan_num, Key &max_key) {
    static thread_local std::mt19937 *generator = nullptr;
    if (!generator)
      generator = new std::mt19937(clock() + pthread_self());
    static thread_local std::uniform_int_distribution<uint64_t> distribution(
        0, 9999);
    auto idx = distribution(*generator);
    uint64_t admission_idx = 10000 * admission_rate_;
    if (idx >= admission_idx) {
      // Just read from remote and return to the application
      int ret = 1;
      auto buffer_page = raw_remote_read(global_node);
      auto cur_leaf = reinterpret_cast<BTreeLeaf<Key, Value> *>(buffer_page);
      if (!cur_leaf->rangeValid(k)) {
        ret = -1;
      } else {
        scan_num = cur_leaf->range_scan(k, scan_num, kv_buffer);
        max_key = cur_leaf->max_limit_;
      }

      bool flag = page_table_->remove_with_lock(
          global_node, reinterpret_cast<void *>(IO_FLAG));
      assert(flag == true);
      return ret;
    }

    return cold_to_hot(global_node, ret_page, parent, child_idx, refresh);
  }

  // -1 means failure and retry
  // 0 means cold to hot succeeds
  // 1 means RPC succeeds
  // A pre-assumption: an IO flag has been inserted into the page table
  int cold_to_hot_with_rpc(GlobalAddress global_node, void **ret_page,
                           NodeBase *parent, unsigned child_idx, bool &refresh,
                           Key k, Value &result, bool &success,
                           RPC_type rpc_type) {

#ifdef LATENCY_COLLECT
    int missing_num = parent->level;
    bool sample = false;
    // thread_local LatencyCollector decision;
    auto flag = decision.caching_or_push(missing_num, sample);
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    if (sample) {
      start = std::chrono::high_resolution_clock::now();
    }

    if (state == 1 && flag) {
      // if (state == 1 && flag && rpc_rate_ != 0) {
      GlobalAddress leaf_addr;
      int ret = 0;
      switch (rpc_type) {
      case RPC_type::LOOKUP:
        ret = global_dsm_->rpc_lookup(global_node, k, result);
        break;

      case RPC_type::UPDATE:
        ret = global_dsm_->rpc_update(global_node, k, result, leaf_addr);
        break;

      case RPC_type::INSERT:
        ret = global_dsm_->rpc_insert(global_node, k, result, leaf_addr);
        break;

      case RPC_type::DELETE:
        ret = global_dsm_->rpc_remove(global_node, k, leaf_addr);
        break;

      default:
        break;
      }

      int ret_flag = 1;
      if (ret <= 0) {
        assert(rpc_type == RPC_type::INSERT);
        ret_flag = -1;
      } else if (ret == 1) {
        success = true;
        // Also check the leaf_addr in cache of compute node and invalidate it
        // Invalidation: just remove it from page table
        page_table_->check_and_remove(leaf_addr);
      } else if (ret == 2) {
        success = false;
        if (rpc_type == RPC_type::INSERT) {
          page_table_->check_and_remove(leaf_addr);
        }
      }

      bool remove_success = page_table_->remove_with_lock(
          global_node, reinterpret_cast<void *>(IO_FLAG));
      assert(remove_success == true);

      if (sample) {
        end = std::chrono::high_resolution_clock::now();
        decision.add_pushdown_latency(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
      }

      return ret_flag;
    }

    // std::cout << "Never hit this?" << std::endl;
    auto ret = cold_to_hot(global_node, ret_page, parent, child_idx, refresh);
    if (sample && ret == 0) {
      end = std::chrono::high_resolution_clock::now();
      decision.add_caching_latency(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
    }
    return ret;
#else
    // Suppose the rpc_rate_ is static
    static thread_local std::mt19937 *generator = nullptr;
    if (!generator)
      generator = new std::mt19937(clock() + pthread_self());
    static thread_local std::uniform_int_distribution<uint64_t> distribution(
        0, 9999);
    auto idx = distribution(*generator);
    uint64_t sample_idx = 10000 * rpc_rate_;
    if (idx < sample_idx) {
      int ret = 0;
      GlobalAddress leaf_addr;
      switch (rpc_type) {
      case RPC_type::LOOKUP:
        ret = global_dsm_->rpc_lookup(global_node, k, result);
        break;

      case RPC_type::UPDATE:
        ret = global_dsm_->rpc_update(global_node, k, result, leaf_addr);
        break;

      case RPC_type::INSERT:
        ret = global_dsm_->rpc_insert(global_node, k, result, leaf_addr);
        break;

      case RPC_type::DELETE:
        ret = global_dsm_->rpc_remove(global_node, k, leaf_addr);
        break;

      default:
        break;
      }

      int ret_flag = 1;
      if (ret <= 0) {
        assert(rpc_type == RPC_type::INSERT);
        ret_flag = -1;
      } else if (ret == 1) {
        success = true;
        // Also check the leaf_addr in cache of compute node and invalidate it
        // Invalidation: just remove it from page table
        page_table_->check_and_remove(leaf_addr);
      } else if (ret == 2) {
        success = false;
        if (rpc_type == RPC_type::INSERT) {
          page_table_->check_and_remove(leaf_addr);
        }
      }

      bool flag = page_table_->remove_with_lock(
          global_node, reinterpret_cast<void *>(IO_FLAG));
      assert(flag == true);
      return ret_flag;
    }

    return cold_to_hot(global_node, ret_page, parent, child_idx, refresh);
#endif
  }

  // -1 means failure and retry
  // 0 means cold to hot succeeds
  int cold_to_hot(GlobalAddress global_node, void **ret_page, NodeBase *parent,
                  unsigned child_idx, bool &refresh) {
    // No sync read is needed because leaf nodes are exclusive
    bool sync_read =
        sync_or_not(reinterpret_cast<BTreeInner<Key> *>(parent), child_idx);
    auto ret = simple_cold_to_hot(global_node, ret_page, parent, child_idx,
                                  refresh, sync_read);
    if (ret == 0) {
      void *old_flag = nullptr;
      auto flag = page_table_->update_with_lock(
          global_node, reinterpret_cast<void *>(*ret_page), &old_flag);
      assert(flag == true);
      assert(reinterpret_cast<uint64_t>(old_flag) == IO_FLAG);
    } else {
      auto flag = page_table_->remove_with_lock(
          global_node, reinterpret_cast<void *>(IO_FLAG));
      assert(flag == true);
    }
    return ret;
  }

  bool get_exclusive_node(GlobalAddress node, NodeBase *target_node) {
    if (target_node->pos_state == 1) {
      // How to enfore that page table is indexing the up-to-date page?
      auto promote_succes =
          hash_table_->try_promote_using_value(node.val, target_node);
      return promote_succes;
    } else if (target_node->pos_state == 2) {
      bool needRestart = false;
      target_node->writeLockOrRestart(needRestart);
      if (needRestart) {
        return false;
      }

      assert(target_node->pos_state == 2);
      auto target_parent =
          reinterpret_cast<BTreeInner<Key> *>(target_node->parent_ptr);
      if (target_parent != nullptr) {
        assert(target_parent->pos_state == 2);
        target_parent->writeLockOrRestart(needRestart);
        if (needRestart) {
          target_node->writeUnlock();
          return false;
        }

        // The following may happen because of SMO
        if (target_node->parent_ptr != target_parent) {
          target_parent->writeUnlock();
          target_node->writeUnlock();
          return false;
        }

        // assert(check_parent_child_info(target_parent, target_node));
        if (!check_parent_child_info(target_parent, target_node)) {
          std::cout << "There is a BUGGGGGGGGGG!!!!!!" << std::endl;
          while (true)
            ;
        }

        auto idx_in_parent = target_parent->findIdx(
            reinterpret_cast<uint64_t>(target_node) | swizzle_tag);
        assert(idx_in_parent != -1);
        unswizzling(target_parent->children[idx_in_parent], target_parent,
                    idx_in_parent, target_node);
        target_parent->writeUnlock();
      }
      assert(target_node->isLocked());
      return true;
    }

    return false;
  }

  void replace_child(BTreeInner<Key> *parent, NodeBase *cur_node,
                     NodeBase *new_node) {
    int idx_in_parent = -1;
    if (parent->level == 255) {
      idx_in_parent = 0;
    } else {
      idx_in_parent =
          parent->findIdx(reinterpret_cast<uint64_t>(cur_node) | swizzle_tag);
    }
    assert(idx_in_parent != -1);
    cur_node->parent_ptr = nullptr;
    new_node->parent_ptr = parent;
    parent->children[idx_in_parent].val =
        reinterpret_cast<uint64_t>(new_node) | swizzle_tag;
  }

  // Lock of cur_node and parent have been acquired, remote_cur_node is in read
  // buffer
  void updatest_replace(NodeBase *cur_node, BTreeInner<Key> *parent,
                        NodeBase *remote_cur_node) {
    fill_local_page_set();
    GlobalAddress remote_addr = cur_node->remote_address;
    auto head_page_bucket = page_table_->get_bucket(remote_addr);
    head_page_bucket->lock_.get_lock();
    auto target_page = page_table_->get(head_page_bucket, remote_addr);
    auto target_node = reinterpret_cast<NodeBase *>(target_page);

    // Case 1
    if (target_node == nullptr) {
      // This case may happen if the up-to-date cur_node has been evicted
      // from the buffer pool
      // GlobalAddress swizzling = remote_addr;
      // cache_insert(swizzling, remote_cur_node, parent, false);
      // NodeBase *new_cur_node = search_in_cache(swizzling);
      NodeBase *new_cur_node =
          reinterpret_cast<NodeBase *>(get_local_page_set());
      assert(new_cur_node != nullptr);
      buffer_to_cache(new_cur_node, remote_cur_node);

      replace_child(parent, cur_node, new_cur_node);
      auto mem_addr = reinterpret_cast<void *>(new_cur_node);
      auto insert_success =
          page_table_->insert(head_page_bucket, remote_addr, mem_addr);
      assert(insert_success == true);
      // Check parent child relation
      assert(check_parent_child_info(parent, new_cur_node));
      new_cur_node->pos_state = 2;
      new_cur_node->writeUnlock();
    } else if (target_node->front_version == cur_node->front_version) {
      assert(target_node == cur_node);
      // Directly do the replacement
      // GlobalAddress swizzling = remote_addr;
      // cache_insert(swizzling, remote_cur_node, parent, false);
      // NodeBase *new_cur_node = search_in_cache(swizzling);
      NodeBase *new_cur_node =
          reinterpret_cast<NodeBase *>(get_local_page_set());
      assert(new_cur_node != nullptr);
      buffer_to_cache(new_cur_node, remote_cur_node);

      replace_child(parent, cur_node, new_cur_node);
      auto mem_addr = reinterpret_cast<void *>(new_cur_node);
      void *old_val = nullptr;
      auto update_success = page_table_->update(head_page_bucket, remote_addr,
                                                mem_addr, &old_val);
      assert(update_success == true);
      assert(old_val == target_page);
      cur_node->obsolete = true;
      assert(check_parent_child_info(parent, new_cur_node));
      new_cur_node->pos_state = 2;
      new_cur_node->writeUnlock();
    } else if (target_node->front_version > cur_node->front_version) {
      // FIXME(BT): this should never happen?
      // cur_node is not up_to_date "cur_node", so we should first move the
      // target node to make it attach to the parent
      auto success = get_exclusive_node(remote_addr, target_node);
      if (success) {
        // if (!check_limit_match(parent, target_node)) {
        if (target_node->min_limit_ == cur_node->min_limit_ &&
            target_node->max_limit_ == cur_node->max_limit_) {
          replace_child(parent, cur_node, target_node);
          assert(check_parent_child_info(parent, target_node));
        }
        target_node->pos_state = 2;
        target_node->writeUnlock();
      }
    }
    head_page_bucket->lock_.release_lock();
  }

  bool remote_to_cache(void *cache_page, GlobalAddress global_node,
                       bool sync_read) {
    NodeBase *buffer_page = nullptr;
    if (sync_read) {
      buffer_page = opt_remote_read(global_node);
      if (buffer_page == nullptr)
        return false;
    } else {
      buffer_page = raw_remote_read(global_node);
    }
    memcpy(reinterpret_cast<char *>(cache_page) + 8,
           reinterpret_cast<char *>(buffer_page) + 8, pageSize - 8);
    return true;
  }

  void buffer_to_cache(NodeBase *cache_page, NodeBase *cur_node) {
    memcpy(reinterpret_cast<char *>(cache_page) + 8,
           reinterpret_cast<char *>(cur_node) + 8, pageSize - 8);
    assert(cache_page->isLocked());
    assert(cache_page->pos_state != 2);
  }

  // -1 means failure and retry
  // 0 means succeeds
  int simple_cold_to_hot(GlobalAddress node, void **ret_page, NodeBase *parent,
                         unsigned child_idx, bool &refresh, bool sync_read) {
    void *page = try_get_empty_page();
    if (page == nullptr) {
      return -1;
    }

    assert(reinterpret_cast<NodeBase *>(page)->pos_state != 2);
    bool IO_success = remote_to_cache(page, node, sync_read);
    auto return_page = reinterpret_cast<NodeBase *>(page);
    assert(parent->level != 255);
    if ((!IO_success) || (!fit_limits(return_page)) ||
        (!new_check_limit_match(parent, return_page, child_idx))) {
      insert_local_set(reinterpret_cast<uint64_t>(return_page));
      refresh = true;
      return -1;
    }

    if (return_page->remote_address != node) {
      std::cout << "Remote node is incorrect when reading it" << std::endl;
      while (true)
        ;
    }

    assert(return_page->pos_state == 0);
    assert(return_page->isLocked());
    assert(return_page->parent_ptr == nullptr);
    *ret_page = return_page;
    return 0;
  }

  void reset(bool flush_dirty) {
    if (flush_dirty)
      flush_all();
    state.store(0);
    inner_miss_ = 0;
    leaf_miss_ = 0;
    full_page_miss_ = 0;
    rdma_write = 0;

    cache_allocator::reset();
    hash_table_->reset();
    page_table_->reset();
  }

  void flush_all() {
    inner_miss_ = 0;
    leaf_miss_ = 0;
    // Flush where are dirty in allocator
    uint64_t start = cache_allocator::instance_->base_address;
    uint64_t page_num = cache_allocator::instance_->page_num_;

    for (uint64_t i = 0; i < page_num; ++i) {
      auto cur_page = reinterpret_cast<NodeBase *>(start + i * pageSize);
      // First fully unswizzle its children
      if (cur_page->dirty) {
        auto page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
        memcpy(page_buffer, cur_page, pageSize);
        auto buffer_page = reinterpret_cast<NodeBase *>(page_buffer);
        fully_unswizzle(buffer_page);
        remote_write(buffer_page->remote_address, buffer_page, true);
        cur_page->dirty = false;
      }
    }
  }

  void check_dirty_in_buffer() {
    uint64_t start = cache_allocator::instance_->base_address;
    uint64_t page_num = cache_allocator::instance_->page_num_;
    uint64_t dirty_page = 0;

    for (uint64_t i = 0; i < page_num; ++i) {
      auto cur_page = reinterpret_cast<NodeBase *>(start + i * pageSize);
      if (cur_page->dirty) {
        dirty_page++;
      }
    }
    std::cout << "The buffer pool has " << dirty_page << " dirty pages"
              << std::endl;
  }

  void statistic_in_buffer() {
    uint64_t start = cache_allocator::instance_->base_address;
    uint64_t page_num = cache_allocator::instance_->page_num_;
    int remote_state = 0;
    int cooling_state = 0;
    int hot_state = 0;
    int local_work_page = 0;
    int hot_leaf = 0;
    int hot_inner = 0;
    int cooling_leaf = 0;
    int cooling_inner = 0;
    int hot_mini = 0;
    int cooling_mini = 0;
    int local_mini = 0;
    uint64_t hot_mini_records = 0;
    uint64_t cooling_mini_records = 0;

    for (uint64_t i = 0; i < page_num * 2; ++i) {
      auto cur_page = reinterpret_cast<NodeBase *>(start + i * pageSize / 2);
      switch (cur_page->pos_state) {
      case 0:
        remote_state++;
        break;
      case 1:
        cooling_state++;
        if (cur_page->type == PageType::BTreeInner) {
          cooling_inner++;
          ++i;
        } else if (cur_page->type == PageType::BTreeLeaf) {
          cooling_leaf++;
          ++i;
        } else {
          cooling_mini++;
          cooling_mini_records += cur_page->count;
        }
        break;
      case 2:
        hot_state++;
        if (cur_page->type == PageType::BTreeInner) {
          hot_inner++;
          ++i;
        } else if (cur_page->type == PageType::BTreeLeaf) {
          hot_leaf++;
          ++i;
        } else {
          hot_mini++;
          hot_mini_records += cur_page->count;
        }
        break;
      case 3:
        local_work_page++;
        ++i;

      default:
        break;
      }
    }
    std::cout << "#hot inner = " << hot_inner << std::endl;
    std::cout << "#hot leaf = " << hot_leaf << std::endl;
    std::cout << "#hot mini = " << hot_mini << std::endl;

    // std::cout << "#entries in one Mini Page = "
    //           << BTreeMini<Key, Value>::maxEntries << std::endl;
    std::cout << "remote_state: " << remote_state << std::endl;
    std::cout << "cooling_state: " << cooling_state << std::endl;
    std::cout << "hot_state: " << hot_state << std::endl;
    std::cout << "local_work_page: " << local_work_page << std::endl;
    std::cout << "local_mini_page: " << local_mini << std::endl;

    std::cout << "hot leaf/mini ratio = "
              << static_cast<double>(hot_leaf + hot_mini / 2) /
                     static_cast<double>(page_num)
              << std::endl;
    // std::cout << "hot mini load factor = "
    //           << hot_mini_records /
    //                  static_cast<double>(hot_mini *
    //                                      BTreeMini<Key, Value>::maxEntries)
    //           << std::endl;
    std::cout << "#cooling inner = " << cooling_inner << std::endl;
    std::cout << "#cooling leaf = " << cooling_leaf << std::endl;
    std::cout << "#cooling mini = " << cooling_mini << std::endl;
    std::cout << "cooling leaf ratio = "
              << static_cast<double>(cooling_leaf + cooling_mini / 2) /
                     static_cast<double>(page_num)
              << std::endl;
    // std::cout << "cooling mini load factor = "
    //           << cooling_mini_records /
    //                  static_cast<double>(cooling_mini *
    //                                      BTreeMini<Key, Value>::maxEntries)
    //           << std::endl;
  }
};

} // namespace cachepush