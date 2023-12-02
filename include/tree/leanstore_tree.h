/*
 * BTreeOLC_child_layout.h - This file contains a modified version that
 *                           uses the key-value pair layout
 *
 * We use this to test whether child node layout will affect performance
 */
#pragma once

#include "../cache/leanstore_cache.h"
#include "../tree_api.h"
#include <atomic>
#include <cassert>
#include <cstring>
#include <immintrin.h>
#include <inttypes.h>
#include <iostream>
#include <limits>
#include <list>
#include <sched.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cachepush {
uint64_t push_down_counter = 0;
uint64_t cache_miss_counter = 0;
uint64_t admission_counter = 0;
uint64_t cache_miss_array[8] = {
    0, 0, 0, 0, 0, 0, 0, 0}; // Collect the cache miss in bottom 5 levels
// In the memory side, we only maintain a BTree without any statistics
// In the client, we maintain a LRU cache
thread_local uint64_t total_nanoseconds = 0;
thread_local uint64_t total_sample_times = 0;

// #define FINE_GRAINED 1

template <class Key, class Value> class BTree : public tree_api<Key, Value> {
public:
  GlobalAddress root_ptr_ptr_;
  GlobalAddress root_lock_ptr_; // To the global lock
  GlobalAddress root;
  OptLock root_lock_; // FIXME(BT): should be removed later
  uint64_t tree_id_;
  DSM *dsm_;
  BTreeInner<Key> *super_root_; // which can be

  // Sharding Information;
  Key left_bound_;
  Key right_bound_;

  Key min_key_;
  Key max_key_;

  // The following statistic are only used for CPU side for debug information
  int leaf_num;
  int inner_num;
  int height;
  // std::atomic<uint64_t> num_rdma_in_refresh;

  CacheManager cache;

  BTree(DSM *dsm, uint64_t tree_id, uint64_t cache_mb, double sample_rate,
        double admission_rate)
      : cache(cache_mb * 1024 * 1024 / pageSize, 0.1, sample_rate,
              admission_rate, &root) {
    std::cout << "start generating tree" << std::endl;
    super_root_ = new BTreeInner<Key>(255, GlobalAddress::Null());
    super_root_->pos_state = 2;
    // super_root_->shared = true;
    dsm_ = dsm;
    tree_id_ = tree_id;
    root_ptr_ptr_ = get_root_ptr_ptr();
    root_lock_ptr_ = get_root_lock_ptr();
    auto root_node = allocate_leaf_node(0);
    printf("Global Root addr = 0x%" PRIx64 "\n", root_node.val);
    update_root_ptr(root_node, GlobalAddress::Null());
    leaf_num = 1;
    inner_num = 0;
    height = 1;
    /* cache_size = cache_mb * 1024 * 1024;
    capacity = cache_size / pageSize;
    cache_num = 0; */
    printf("Root addr = 0x%" PRIx64 "\n", root.val);
    std::cout << "finish generating tree" << std::endl;
  }

  // With partition information
  BTree(DSM *dsm, uint64_t tree_id, uint64_t cache_mb, double sample_rate,
        double admission_rate, std::vector<Key> partition_info,
        int num_partitions)
      : cache(cache_mb * 1024 * 1024 / pageSize, 0.1, sample_rate,
              admission_rate, &root) {
    std::cout << "start generating tree" << std::endl;
    super_root_ = new BTreeInner<Key>(255, GlobalAddress::Null());
    super_root_->pos_state = 2;
    // super_root_->shared = true;
    dsm_ = dsm;
    tree_id_ = tree_id;
    root_ptr_ptr_ = get_root_ptr_ptr();
    root_lock_ptr_ = get_root_lock_ptr();

    std::cout << "num_partitions = " << num_partitions << std::endl;

    if (num_partitions == 1) {
      auto root_node = allocate_leaf_node(0);
      // printf("Global Root addr = 0x%" PRIx64 "\n", root_node);
      update_root_ptr(root_node, GlobalAddress::Null());
      leaf_num = 1;
      inner_num = 0;
      height = 1;
    } else {
      assert(static_cast<size_t>(num_partitions + 1) == partition_info.size());
      // First allocate a new inner node
      // std::cout << "Before allocating node" << std::endl;
      GlobalAddress newInner = allocate_node(0);
      std::cout << "--------ORG root ptr = " << newInner << std::endl;
      auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
      auto new_mem = new (page_buffer) BTreeInner<Key>(1, newInner);
      new_mem->count = num_partitions - 1;
      new_mem->children[0] =
          allocate_leaf_node(partition_info[0], partition_info[1] - 1, 0);
      // std::cout << "Leaf 0 left = " << partition_info[0]
      //           << "right = " << partition_info[1] << std::endl;
      for (int i = 1; i < num_partitions - 1; ++i) {
        new_mem->keys[i - 1] = partition_info[i] - 1;
        new_mem->children[i] = allocate_leaf_node(partition_info[i] - 1,
                                                  partition_info[i + 1] - 1, 0);
      }

      new_mem->keys[num_partitions - 2] =
          partition_info[num_partitions - 1] - 1;
      new_mem->children[num_partitions - 1] =
          allocate_leaf_node(partition_info[num_partitions - 1] - 1,
                             partition_info[num_partitions], 0);
      new_mem->typeVersionLockObsolete = 0;
      dsm_->write_sync(page_buffer, newInner, pageSize);
      update_root_ptr(newInner, GlobalAddress::Null());
      leaf_num = num_partitions;
      inner_num = 1;
      height = 2;
    }

    /* cache_size = cache_mb * 1024 * 1024;
    capacity = cache_size / pageSize;
    cache_num = 0; */
    // printf("Root addr = 0x%" PRIx64 "\n", root);
    std::cout << "finish generating tree" << std::endl;
  }

  void update_root_ptr(GlobalAddress new_root, GlobalAddress old_root) {
    auto cas_buffer = (dsm_->get_rbuf(0)).get_cas_buffer();
    bool res = dsm_->cas_sync(root_ptr_ptr_, get_global_address(old_root).val,
                              get_global_address(new_root).val, cas_buffer);
    if (res) {
      std::cout << "Success: tree root pointer value "
                << get_global_address(new_root) << std::endl;
      root = new_root;
    } else {
      // Only one thread could succeed
      LOG("Fail to install the root");
      // read the root value back to local
      auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
      dsm_->read_sync(page_buffer, root_ptr_ptr_, sizeof(GlobalAddress),
                      nullptr);
      root = *reinterpret_cast<GlobalAddress *>(page_buffer);
      std::cout << "Fail: tree root pointer value from remote "
                << get_global_address(root) << std::endl;
    }

    super_root_->children[0] = root;
    if (get_memory_address(root) != nullptr) {
      super_root_->set_bitmap(0);
    }
  }

  void get_newest_root() {
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    dsm_->read_sync(page_buffer, root_ptr_ptr_, sizeof(GlobalAddress), nullptr);
    auto new_root = *reinterpret_cast<GlobalAddress *>(page_buffer);
    if (new_root.val != get_global_address(root).val) {
      root = new_root;
    }
    // root = *reinterpret_cast<GlobalAddress *>(page_buffer);
    std::cout << "IMPORTANT: my root is " << get_global_address(root).val
              << std::endl;
  }

  void makeRoot(Key k, GlobalAddress leftChild, GlobalAddress rightChild,
                uint8_t level) {
    GlobalAddress new_root;
    auto remote_left = get_global_address(leftChild);
    if ((level % megaLevel) == 0) {
      // new_root = allocate_node(remote_left.nodeID + 1);
      new_root = allocate_node(dsm_->get_random_id(remote_left.nodeID));
    } else {
      new_root = allocate_node(remote_left.nodeID);
    }
    // auto page_buffer = get_sibling_buffer();
    auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
    auto inner = new (page_buffer) BTreeInner<Key>(level, new_root);
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    inner->shared = true;
    cache.cache_insert(new_root, inner, nullptr);
    auto mem_root = cache.search_in_cache(new_root);
    assert(mem_root != nullptr);
    mem_root->pos_state = 2;
    auto memory_left = cache.search_in_cache(leftChild);
    if (memory_left != nullptr) {
      memory_left->parent_ptr = mem_root;
      reinterpret_cast<BTreeInner<Key> *>(mem_root)->set_bitmap(0);
    }
    auto memory_right = cache.search_in_cache(rightChild);
    if (memory_right != nullptr) {
      memory_right->parent_ptr = mem_root;
      reinterpret_cast<BTreeInner<Key> *>(mem_root)->set_bitmap(1);
    }
    remote_write_unswizzle(mem_root, true);
    update_root_ptr(new_root, root); // This must succeed!
    mem_root->parent_ptr = super_root_;
    height = mem_root->level + 1;
    mem_root->writeUnlock();
    std::cout << "Height = " << height << std::endl;
    // Also need to bring the new root into cache
    //++height;
    //++inner_num;
  }

  GlobalAddress get_root_ptr_ptr() {
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset =
        define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id_;
    return addr;
  }

  GlobalAddress get_root_lock_ptr() {
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset = define::kRootPointerStoreOffest +
                  sizeof(GlobalAddress) * (tree_id_ + 1);
    return addr;
  }

  // allocate distributed node using RDMA
  GlobalAddress allocate_node() {
    // GlobalAddress node_addr;
    // auto ret = global_allocator::allocate();
    // if (ret == nullptr) {
    //   std::cout << "External Mem allocation failed" << std::endl;
    //   exit(0);
    // }
    // node_addr.val = reinterpret_cast<uint64_t>(ret);
    // return node_addr;
    auto node_addr = dsm_->alloc(pageSize);
    return node_addr;
  }

  GlobalAddress allocate_node(uint32_t node_id) {
    // GlobalAddress node_addr;
    // auto ret = global_allocator::allocate();
    // if (ret == nullptr) {
    //   std::cout << "External Mem allocation failed" << std::endl;
    //   exit(0);
    // }
    // node_addr.val = reinterpret_cast<uint64_t>(ret);
    // return node_addr;
    auto node_addr = dsm_->alloc(pageSize, node_id);
    return node_addr;
  }

  // FIXME(BT): this should be removed
  GlobalAddress allocate_leaf_node() {
    auto node_addr = dsm_->alloc(pageSize);
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    auto new_mem = new (page_buffer) BTreeLeaf<Key, Value>(node_addr);
    new_mem->typeVersionLockObsolete = 0;
    dsm_->write_sync(page_buffer, node_addr, pageSize);
    return node_addr;
  }

  GlobalAddress allocate_leaf_node(uint32_t node_id) {
    auto node_addr = dsm_->alloc(pageSize, node_id);
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    auto new_mem = new (page_buffer) BTreeLeaf<Key, Value>(node_addr);
    new_mem->typeVersionLockObsolete = 0;
    dsm_->write_sync(page_buffer, node_addr, pageSize);
    return node_addr;
  }

  GlobalAddress allocate_leaf_node(Key min_limit, Key max_limit,
                                   uint32_t node_id = 0) {
    auto node_addr = dsm_->alloc(pageSize, node_id);
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    new (page_buffer) BTreeLeaf<Key, Value>(node_addr);
    auto cur_node = reinterpret_cast<BTreeLeaf<Key, Value> *>(page_buffer);
    cur_node->typeVersionLockObsolete = 0;
    cur_node->min_limit_ = min_limit;
    cur_node->max_limit_ = max_limit;
    dsm_->write_sync(page_buffer, node_addr, pageSize);
    return node_addr;
  }

  // Below is the statistic information
  void get_basic() {
    std::cout << "#leaf nodes = " << leaf_num << std::endl;
    std::cout << "leaf size(MB) = " << leaf_num * pageSize / (1024.0 * 1024.0)
              << std::endl;
    std::cout << "#inner nodes = " << inner_num << std::endl;
    std::cout << "inner size(MB) = " << inner_num * pageSize / (1024.0 * 1024.0)
              << std::endl;
    std::cout << "Tree height = " << height << std::endl;
    std::cout << "Cache capacity = " << cache.capacity << std::endl;
    std::cout << "max. leaf entries = " << BTreeLeaf<Key, Value>::maxEntries
              << std::endl;
    std::cout << "max. inner entries = " << BTreeInner<Key>::maxEntries
              << std::endl;

    std::cout << "Inner page size = " << sizeof(BTreeInner<Key>) << std::endl;
    std::cout << "Leaf page size = " << sizeof(BTreeLeaf<Key, Value>)
              << std::endl;
  }

  void get_node_info() {
    std::cout << "inner node size (B) = " << sizeof(BTreeInner<Key>)
              << std::endl;
    std::cout << "leaf node size (B) = " << sizeof(BTreeLeaf<Key, Value>)
              << std::endl;
  }

  uint64_t get_inner_miss() { return cache.inner_miss_; }

  uint64_t get_leaf_miss() { return cache.leaf_miss_; }

  uint64_t get_miss() { return cache.inner_miss_ + cache.leaf_miss_; }

  uint64_t get_write_rdma() { return cache.rdma_write; }

  uint64_t get_all_rdma() {
    return (cache.inner_miss_ + cache.leaf_miss_ + cache.rdma_write);
  }

  void set_left_bound(Key left) {
    left_bound_ = left;
    cache.buffer_min_limit_ = left;
  }

  void set_right_bound(Key right) {
    right_bound_ = right;
    cache.buffer_max_limit_ = right;
  }

  void set_bound(Key left, Key right) {
    set_left_bound(left);
    set_right_bound(right);
  }

  int get_node_ID(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      auto mem_node = reinterpret_cast<NodeBase *>(node & swizzle_hide);
      return mem_node->remote_address.nodeID;
    }
    return global_node.nodeID;
  }

  GlobalAddress get_global_address(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      auto mem_node = reinterpret_cast<NodeBase *>(node & swizzle_hide);
      return mem_node->remote_address;
    }
    return global_node;
  }

  void *get_memory_address(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      return reinterpret_cast<void *>(node & swizzle_hide);
    }
    return nullptr;
  }

  bool try_get_remote_lock(GlobalAddress lock) {
    auto tag = dsm_->getThreadTag();
    auto cas_buffer = (dsm_->get_rbuf(0)).get_cas_buffer();
    return dsm_->cas_sync(lock, 0, tag, cas_buffer);
  }

  bool test_remote_lock_set(GlobalAddress lock) {
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    dsm_->read_sync(page_buffer, lock, sizeof(uint64_t), nullptr);
    auto lock_val = *reinterpret_cast<uint64_t *>(page_buffer);
    return (lock_val != 0);
  }

  void get_remote_lock(GlobalAddress lock) {
    auto tag = dsm_->getThreadTag();
    auto cas_buffer = (dsm_->get_rbuf(0)).get_cas_buffer();
    while (true) {
      auto ret = dsm_->cas_sync(lock, 0, tag, cas_buffer);
      if (ret)
        break;
    }
  }

  void release_remote_lock(GlobalAddress lock) {
    auto cas_buffer = (dsm_->get_rbuf(0)).get_cas_buffer();
    *cas_buffer = 0;
    dsm_->write_sync(reinterpret_cast<char *>(cas_buffer), lock,
                     sizeof(uint64_t));
  }

  bool is_local_remote_match(NodeBase *local_node, GlobalAddress remote_node,
                             bool verbose = false) {
    // Just check whether the version change
    auto remote_mem =
        reinterpret_cast<NodeBase *>(raw_remote_read(remote_node, 16));
    if (local_node->front_version != remote_mem->front_version) {
      if (verbose) {
        std::cout << "Local node version = " << local_node->front_version
                  << std::endl;
        std::cout << "Remote node version = " << remote_mem->front_version
                  << std::endl;
      }
      return false;
    }
    return true;
  }

  // bulk_load should be only executed by one computing node
  void bulk_load(std::vector<std::pair<Key, Value>> &array, uint64_t num,
                 bool sorted = true) {
    if (dsm_->getMyNodeID() == 0) {
      set_left_bound(std::numeric_limits<Key>::min());
      set_right_bound(std::numeric_limits<Key>::max());
      for (uint64_t i = 0; i < num; ++i) {
        insert_single(array[i].first, array[i].second);
      }

      min_key_ = array[0].first;
      max_key_ = array[num - 1].first;
    }
  }

  void bulk_load(Key *bulk_array, uint64_t bulk_load_num) {
    if (dsm_->getMyNodeID() == 0) {
      set_left_bound(std::numeric_limits<Key>::min());
      set_right_bound(std::numeric_limits<Key>::max());
      std::sort(bulk_array, bulk_array + bulk_load_num);
      for (uint64_t i = 0; i < bulk_load_num; ++i) {
        // std::cout << "Insert " << i << " key: " << bulk_array[i] <<
        // std::endl;
        insert_single(bulk_array[i], bulk_array[i] + 1);
      }

      min_key_ = bulk_array[0];
      max_key_ = bulk_array[bulk_load_num - 1];
    }
  }

  void set_shared(std::vector<Key> &bound) {
    // cache_allocator::Protect();
    super_root_->shared = true;
    for (auto k : bound) {
      std::cout << "Shared bound = " << k << std::endl;
      while (true) {
        bool needRestart = false;
        bool refresh = false;
        uint64_t versionNode = super_root_->readLockOrRestart(needRestart);
        assert(needRestart == false);
        // NodeBase *mem_node = cache.cache_get(root, &root_lock_, needRestart);
        NodeBase *mem_node = cache.search_in_cache(root);
        if (mem_node == nullptr) {
          load_newest_root(versionNode, needRestart);
          continue;
        }

        assert(refresh == false);

        // Another condition: mem_node->level >= megaLevel
        while (mem_node->type == PageType::BTreeInner) {
          auto inner = static_cast<BTreeInner<Key> *>(mem_node);
          needRestart = false;
          versionNode = mem_node->readLockOrRestart(needRestart);
          assert(needRestart == false);
          inner->shared = true;
          inner->dirty = true;
          std::cout << "Level " << static_cast<int>(mem_node->level)
                    << ": min_limit = " << mem_node->min_limit_
                    << ", max_limit = " << mem_node->max_limit_ << std::endl;
          auto idx = inner->lowerBound(k);
          // mem_node = cache.cache_get(inner->children[idx],
          //                            reinterpret_cast<OptLock *>(inner),
          //                            needRestart, mem_node, idx,
          //                            versionNode);
          mem_node = new_get_mem_node(inner->children[idx], inner, idx,
                                      versionNode, needRestart, refresh, true);
          assert(mem_node != nullptr);
          assert(refresh == false);
        }
        break;
      }
    }
    // cache_allocator::Unprotect();
  }

  void yield(int count) {
    if (count > 3)
      sched_yield();
    else
      _mm_pause();
  }

  // [Left_bound, right_bound) // index range
  // node range: (min_limit, max_limit]
  bool is_private(NodeBase *inner) {
    if (inner->min_limit_ >= left_bound_ && inner->max_limit_ < right_bound_) {
      return true;
    }
    return false;
  }

  //
  // Only used for bulkloading
  GlobalAddress recursive_movement_single(GlobalAddress node,
                                          NodeBase *mem_node,
                                          int cur_mega_level, int node_id) {
    // Suppose mem_node has been locked here
    if (cur_mega_level > 0) {
      assert(mem_node->type == PageType::BTreeInner);
      auto inner = static_cast<BTreeInner<Key> *>(mem_node);
      int count = inner->count;
      // Currently inner has been locked, to make sure it will not be evicted
      for (int i = 0; i <= count; ++i) {
        auto next_node = inner->children[i];
        uint64_t next_node_val = next_node.val;
        NodeBase *next_mem_node = nullptr;
        if (next_node_val & swizzle_tag) {
          // In-memory
          next_mem_node =
              reinterpret_cast<NodeBase *>(next_node_val & swizzle_hide);
        } else {
          // From external or in cooling
          bool restart = false;
          bool refresh = false;
          next_mem_node = new_get_mem_node_without_lock(
              inner->children[i], inner, i, restart, refresh, true);
          assert(next_mem_node != nullptr);
        }

        bool needRestart = false;
        next_mem_node->writeLockOrRestart(needRestart);
        assert(needRestart == false);
        auto new_node = recursive_movement_single(next_mem_node->remote_address,
                                                  next_mem_node,
                                                  cur_mega_level - 1, node_id);
        inner->children[i] = new_node;
        assert(check_parent_child_info(
            inner, reinterpret_cast<NodeBase *>(new_node.val & swizzle_hide)));
        assert(reinterpret_cast<void *>(next_mem_node) ==
               reinterpret_cast<void *>(new_node.val & swizzle_hide));
        next_mem_node->writeUnlock();
      }

      if (node.nodeID != node_id) {
        // Conduct the movement among memory servers
        auto new_node = allocate_node(node_id);
        mem_node->remote_address = new_node;
        // FIXME(BT): we should invalidate the obsolete node in remote memory
        remote_write_unswizzle(mem_node, true);
        // Upate the page table
        void *new_mem_node = reinterpret_cast<void *>(mem_node);
        auto ret = cache.page_table_->insert_with_lock(new_node, new_mem_node);
        assert(ret == true);
        ret = cache.page_table_->remove_with_lock(node, mem_node);
        assert(ret == true);
      }
    } else {
      if (node.nodeID == node_id) {
        std::cout << "Existing ID = " << node.nodeID << std::endl;
        std::cout << "New ID = " << node_id << std::endl;
      }
      assert(node.nodeID != node_id);
      // last-level node movement
      auto new_node = allocate_node(node_id);
      mem_node->remote_address = new_node;
      remote_write_unswizzle(mem_node, true);
      void *new_mem_node = reinterpret_cast<void *>(mem_node);
      auto ret = cache.page_table_->insert_with_lock(new_node, new_mem_node);
      assert(ret == true);
      ret = cache.page_table_->remove_with_lock(node, mem_node);
      assert(ret == true);
      // Update the page table
    }

    GlobalAddress return_node;
    return_node.val = reinterpret_cast<uint64_t>(mem_node) | swizzle_tag;
    return return_node;
  }

  // Used in bulk-loading algorithm
  // Single-thread implementation of insert, which uses new
  void insert_single(Key k, Value v) {
    // cache_allocator::Protect();
  restart:
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionNode = super_root_->readLockOrRestart(needRestart);
    assert(needRestart == false);
    NodeBase *mem_node = cache.search_in_cache(root);
    if (mem_node == nullptr) {
      // Load newest root
      load_newest_root(versionNode, needRestart);
      goto restart;
    }
    assert(mem_node != nullptr);

    // Parent of current node
    BTreeInner<Key> *parent = super_root_;
    while (mem_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(mem_node);
      // inner->validate_bitmap_correctness(1);
      // Split eagerly if full
      if (inner->isFull()) {
        ++inner_num;
        if (inner->level == megaLevel - 1) {
          // if ((inner->level % megaLevel) == megaLevel - 1) {
          // level = 1 incur; Sub-tree is splitting, we
          // should reorganize the sb-tree to another machine
          Key sep;
          // thread_local uint32_t next_node_id = 1;
          int next_node_id = dsm_->get_random_id(inner->remote_address.nodeID);
          GlobalAddress newInner = allocate_node(next_node_id);
          //++next_node_id;
          auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
          auto new_mem_node =
              new (page_buffer) BTreeInner<Key>(inner->level, newInner);
          inner->writeLockOrRestart(needRestart);
          assert(needRestart == false);
          inner->split(sep, new_mem_node);
          cache.cache_insert(newInner, new_mem_node, parent);
          auto mem_new_inner = cache.search_in_cache(newInner);
          assert(mem_new_inner != nullptr);
          assert(mem_new_inner->isLocked());
          assert(mem_new_inner->pos_state == 4);
          mem_new_inner->pos_state = 2;
          update_all_parent_ptr(mem_new_inner);
          if (parent != super_root_) {
            parent->insert(sep, newInner);
          } else {
            GlobalAddress node;
            node.val = reinterpret_cast<uint64_t>(inner) | swizzle_tag;
            makeRoot(sep, node, newInner, inner->level + 1);
          }
          inner->writeUnlock();
          // We do not release the lock for mem_new_inner, instead we do the
          // recursive movement
          auto new_mega_root = get_global_address(newInner);
          recursive_movement_single(new_mega_root,
                                    reinterpret_cast<NodeBase *>(mem_new_inner),
                                    megaLevel - 1, new_mega_root.nodeID);
          mem_new_inner->writeUnlock();
        } else {
          Key sep;
          GlobalAddress newInner =
              allocate_node(mem_node->remote_address.nodeID);
          auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
          auto new_mem =
              new (page_buffer) BTreeInner<Key>(inner->level, newInner);
          inner->writeLockOrRestart(needRestart);
          assert(needRestart == false);
          inner->split(sep, new_mem);
          cache.cache_insert(newInner, new_mem, parent);
          auto mem_new_inner = cache.search_in_cache(newInner);
          assert(mem_new_inner != nullptr);
          assert(mem_new_inner->isLocked());
          assert(mem_new_inner->pos_state == 4);
          mem_new_inner->pos_state = 2;
          update_all_parent_ptr(mem_new_inner);

          if (parent != super_root_) {
            parent->insert(sep, newInner);
          } else {
            GlobalAddress node;
            node.val = reinterpret_cast<uint64_t>(inner) | swizzle_tag;
            makeRoot(sep, node, newInner, inner->level + 1);
          }
          inner->writeUnlock();
          mem_new_inner->writeUnlock();
        }

        // Unlock and restart
        goto restart;
      }

      parent = inner;
      uint64_t versionNode = mem_node->readLockOrRestart(needRestart);
      auto idx = inner->lowerBound(k);
      mem_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, true);
      assert(refresh == false);
      if (needRestart)
        goto restart;
      assert(mem_node != nullptr);
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
    assert(rangeValid(k, leaf->min_limit_, leaf->max_limit_));

    // Split leaf if full
    if (leaf->count == leaf->maxEntries) {
      //  Split
      Key sep;
      auto newLeaf = allocate_node(mem_node->remote_address.nodeID);
      auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
      auto new_mem = new (page_buffer) BTreeLeaf<Key, Value>(newLeaf);
      // leaf->pos_state = 4;
      leaf->writeLockOrRestart(needRestart);
      assert(needRestart == false);
      leaf->split(sep, new_mem, newLeaf);
      // dsm_write_sync(page_buffer, newLeaf, pageSize);
      new_mem->typeVersionLockObsolete = 0;
      dsm_->write_sync(page_buffer, newLeaf, pageSize);
      ++leaf_num;
      if (parent != super_root_) {
        parent->insert(sep, newLeaf);
      } else {
        GlobalAddress node;
        node.val = reinterpret_cast<uint64_t>(leaf) | swizzle_tag;
        makeRoot(sep, node, newLeaf, leaf->level + 1);
      }
      leaf->writeUnlock();
      goto restart;
    } else {
      // Cache lazy sync to remote
      leaf->insert(k, v);
    }
    // cache_allocator::Unprotect();
  }

  GlobalAddress get_remote_root() {
    auto page_buffer = (dsm_->get_rbuf(0)).get_page_buffer();
    dsm_->read_sync(page_buffer, root_ptr_ptr_, sizeof(GlobalAddress), nullptr);
    auto new_root = *reinterpret_cast<GlobalAddress *>(page_buffer);
    return new_root;
  }

  // First acquire the local lock and then remote lock
  void full_IO_lock_with_check(NodeBase *mem_node, uint64_t node_version,
                               bool &needRestart) {
    mem_node->upgradeToIOLockOrRestart(node_version, needRestart);
    if (needRestart) {
      return;
    }

    if (!try_get_remote_lock(mem_node->remote_address)) {
      needRestart = true;
      mem_node->IOUnlock();
      return;
    }

    if (!is_local_remote_match(mem_node, mem_node->remote_address)) {
      release_remote_lock(mem_node->remote_address);
      mem_node->IOUnlock();
      needRestart = true;
    }
  }

  void full_lock_with_check(NodeBase *mem_node, uint64_t node_version,
                            bool &needRestart, bool verbose = false) {
    mem_node->upgradeToWriteLockOrRestart(node_version, needRestart);
    if (needRestart) {
      if (verbose) {
        std::cout << "False because local lock fail" << std::endl;
      }
      return;
    }

    if (verbose) {
      auto remote_node = raw_remote_read(mem_node->remote_address);
      std::cout << "Inside Local val = "
                << *reinterpret_cast<uint64_t *>(remote_node) << std::endl;
    }

    if (!try_get_remote_lock(mem_node->remote_address)) {
      needRestart = true;
      mem_node->writeUnlock();
      if (verbose) {
        std::cout << "False because remote lock fail" << std::endl;
      }
      return;
    }

    if (!is_local_remote_match(mem_node, mem_node->remote_address, verbose)) {
      release_remote_lock(mem_node->remote_address);
      mem_node->writeUnlock();
      if (verbose) {
        std::cout << "False because local and remote do not match" << std::endl;
      }
      needRestart = true;
    }
  }

  // first release remote lock and then local lock
  void full_unlock(NodeBase *mem_node) {
    release_remote_lock(mem_node->remote_address);
    mem_node->writeUnlock();
  }

  void full_IO_unlock(NodeBase *mem_node) {
    release_remote_lock(mem_node->remote_address);
    mem_node->IOUnlock();
  }

  void remote_write_unswizzle(NodeBase *mem_node, bool clear_lock = false) {
    auto page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
    if (page_buffer != reinterpret_cast<char *>(mem_node)) {
      memcpy(page_buffer, mem_node, pageSize);
    }
    auto buffer_node = reinterpret_cast<NodeBase *>(page_buffer);
    if (!clear_lock) {
      buffer_node->typeVersionLockObsolete = dsm_->getThreadTag();
    }
    fully_unswizzle(buffer_node);
    remote_write(buffer_node->remote_address, buffer_node, clear_lock);
    mem_node->dirty = false;
  }

  // In this function, suppose the parent has been locked
  void check_parent_global_conflict(BTreeInner<Key> *parent,
                                    GlobalAddress remote_addr,
                                    uint64_t org_version, bool &needRestart) {
    // FIXME(BT): this root check is not successful
    if (parent == super_root_) {
      if (test_remote_lock_set(root_lock_ptr_)) {
        needRestart = true;
        return;
      }
      auto remote_root = get_remote_root();
      auto cur_root = get_global_address(root);
      if (remote_root.val != cur_root.val) {
        needRestart = true;
      }
    } else {
      auto remote_parent_node =
          reinterpret_cast<BTreeInner<Key> *>(raw_remote_read(remote_addr));
      check_global_conflict(remote_parent_node, org_version, needRestart);
      // assert(remote_parent_node->remote_address == parent->remote_address);
    }
  }

  void parent_read_unlock(BTreeInner<Key> *parent, uint64_t versionParent,
                          bool &needRestart) {
    if (parent == super_root_) {
      // Super root
      auto cur_root = get_global_address(root);
      bool shared = parent->isShared();
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        return;
      }

      if (shared) {
        if (test_remote_lock_set(root_lock_ptr_)) {
          needRestart = true;
          return;
        }

        auto remote_root = get_remote_root();
        if (remote_root.val != cur_root.val) {
          needRestart = true;
          return;
        }
      }
    } else {
      // Normal parent
      // Local check
      auto parent_remote_addr = parent->remote_address;
      auto parent_version = parent->front_version;
      bool shared = parent->isShared();
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        return;
      }

      if (shared) {
        auto remote_parent_node = reinterpret_cast<BTreeInner<Key> *>(
            raw_remote_read(parent_remote_addr));
        check_global_conflict(remote_parent_node, parent_version, needRestart);
        if (needRestart) {
          return;
        }
      }
    }
  }

  // Reset the shared state cur_node also make all possible children connected
  // to this cur_node
  bool iterate_check_private(BTreeInner<Key> *inner) {
    int count = inner->count;
    bool needRestart = false;
    for (int i = 0; i <= count; ++i) {
      bool refresh = false;
      auto next_node = new_get_mem_node_without_lock(
          inner->children[i], inner, i, needRestart, refresh, true);
      if (next_node == nullptr || next_node->isShared()) {
        return false;
      }
      // assert(needRestart == false);
    }

    return true;
  }

  void new_refresh_from_root(Key k) {
    // refresh until we entered into the non-shared node (e.g., leaf)
    // A solution: all new nodes can be treated as shared node!
    int restartCount = 0;
    bool verbose = false;
  restart:
    if (restartCount++)
      yield(restartCount);

    bool needRestart = false;
    bool refresh = false;
    NodeBase *cur_node = super_root_;
    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    load_newest_root(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    BTreeInner<Key> *parent = reinterpret_cast<BTreeInner<Key> *>(cur_node);
    uint64_t versionParent = versionNode;
    cur_node = cache.search_in_cache(root);
    assert(cur_node != nullptr);

    versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    // && cur_node->isShared()
    while (cur_node->type == PageType::BTreeInner) {
      auto inner = reinterpret_cast<BTreeInner<Key> *>(cur_node);
      // Compare local with remote node
      if (cur_node->isShared()) {
        auto remote_addr = cur_node->remote_address;
        parent->checkOrRestart(versionParent, needRestart);
        if (needRestart) {
          goto restart;
        }

        auto remote_cur_node = opt_remote_read_by_sibling(remote_addr);
        if (remote_cur_node == nullptr) {
          goto restart;
        }

        // Replacement for the outdated node
        if (remote_cur_node->front_version > cur_node->front_version) {
          // FIXME(BT): is this impossible?
          if (!cache.fit_limits(remote_cur_node)) {
            goto restart;
          }

          auto parent_remote_addr = parent->remote_address;
          auto parent_version = parent->front_version;
          parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
          if (needRestart) {
            goto restart;
          }

          // Global check to check parent is updatest
          check_parent_global_conflict(parent, parent_remote_addr,
                                       parent_version, needRestart);
          if (needRestart) {
            parent->writeUnlock();
            goto restart;
          }

          // Do the replacement
          cur_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
          if (needRestart) {
            parent->writeUnlock();
            goto restart;
          }

          if (parent != super_root_) {
            assert(check_limit_match(parent, cur_node, true));
          }

          assert(cur_node->parent_ptr == parent);
          assert(cur_node->isShared());
          cache.updatest_replace(cur_node, parent, remote_cur_node);
          if (parent->level == 255) {
            root = parent->children[0];
          }
          cur_node->writeUnlock();
          parent->writeUnlock();
          goto restart;
        }

        // Test whether we need to reset its shared state
        // Shared => private
        if (is_private(cur_node)) {
          // Flush this node to remote memory
          full_IO_lock_with_check(cur_node, versionNode, needRestart);
          if (needRestart) {
            goto restart;
          }
          assert(cur_node->front_version == remote_cur_node->front_version);
          auto flag = iterate_check_private(
              reinterpret_cast<BTreeInner<Key> *>(cur_node));
          if (flag) {
            // Increase from IO lock to write lock
            cur_node->IOUpgradeToWriteLockOrRestart(needRestart);
            assert(needRestart == false);
            cur_node->shared = false;
            remote_write(remote_cur_node->remote_address, remote_cur_node,
                         false);
            full_unlock(cur_node);
            goto restart;
          }
          full_IO_unlock(cur_node);
          // goto restart;
        }

        // cur_node is already up-to-date
        if (inner->isFull()) {
          // start the SMO logic
          if (parent == super_root_) {
            parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
            if (needRestart) {
              goto restart;
            }

            if (!try_get_remote_lock(root_lock_ptr_)) {
              parent->writeUnlock();
              goto restart;
            }

            auto remote_root = get_remote_root();
            if (remote_root != get_global_address(root)) {
              release_remote_lock(root_lock_ptr_);
              parent->writeUnlock();
              goto restart;
            }
          } else {
            assert(parent->isShared());
            full_lock_with_check(parent, versionParent, needRestart, verbose);
            if (needRestart) {
              goto restart;
            }
          }

          full_lock_with_check(inner, versionNode, needRestart);
          if (needRestart) {
            if (parent == super_root_) {
              release_remote_lock(root_lock_ptr_);
              parent->writeUnlock();
            } else {
              full_unlock(parent);
            }
            goto restart;
          }

          Key sep;
#ifdef FINE_GRAINED
          GlobalAddress newInner = allocate_node();
#else
          GlobalAddress newInner;
          if (cur_node->level <= megaLevel - 1) {
            newInner = allocate_node(cur_node->remote_address.nodeID);
          } else {
            // Allocate in other memory nodes
            thread_local uint32_t next_node_id = 0;
            // newInner = allocate_node(cur_node->remote_address.nodeID + 1);
            newInner = allocate_node(next_node_id);
            ++next_node_id;
          }
#endif
          auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
          auto new_mem =
              new (page_buffer) BTreeInner<Key>(inner->level, newInner);
          inner->split(sep, new_mem);
          cache.cache_insert(newInner, new_mem, parent);
          auto mem_new_inner = cache.search_in_cache(newInner);
          assert(mem_new_inner != nullptr);
          assert(mem_new_inner->isLocked());
          mem_new_inner->pos_state = 2;
          mem_new_inner->shared = true;
          update_all_parent_ptr(mem_new_inner);

          // new_reset_shared_state(inner);
          // new_reset_shared_state(mem_new_inner);
          // FIXME(BT): how to resolve shared state during split
          if (parent == super_root_) {
            std::cout
                << "---------------- Start Split the root node ---------------"
                << std::endl;
            GlobalAddress node;
            node.val = reinterpret_cast<uint64_t>(inner) | swizzle_tag;
            remote_write_unswizzle(mem_new_inner, true);
            remote_write_unswizzle(inner);

            std::cout << "Left child min_limit = " << inner->min_limit_
                      << ", max_limit = " << inner->max_limit_ << std::endl;
            std::cout << "Right child min_limit = " << mem_new_inner->min_limit_
                      << ", max_limit = " << mem_new_inner->max_limit_
                      << std::endl;

            makeRoot(sep, node, newInner, inner->level + 1);
            mem_new_inner->writeUnlock();
            full_unlock(inner);
            release_remote_lock(root_lock_ptr_);
            parent->writeUnlock();
            std::cout
                << "---------------- Finish Split the root node ---------------"
                << std::endl;
          } else {
            parent->insert(sep, newInner);
            remote_write_unswizzle(mem_new_inner, true);
            remote_write_unswizzle(inner);
            remote_write_unswizzle(parent);
            mem_new_inner->writeUnlock();
            full_unlock(inner);
            full_unlock(parent);
          }

          goto restart;
        }
      }

      parent_read_unlock(parent, versionParent, needRestart);
      if (needRestart)
        goto restart;

      parent = inner;
      versionParent = versionNode;
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], parent, idx,
                                  versionParent, needRestart, refresh, true);
      if (needRestart) {
        goto restart;
      }

      assert(cur_node != nullptr);
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) {
        goto restart;
      }
    }
  }

  // Verify whether this node is outdated and full, if not, we aquire the lock
  // if false, restart from root
  // ret: true, means the lock of this node is acuiqred
  bool verify_and_acquire_lock(NodeBase *cur_node, uint64_t org_version,
                               bool &refresh_cache) {
    refresh_cache = false;
    bool needRestart = false;
    cur_node->upgradeToWriteLockOrRestart(org_version, needRestart);
    if (needRestart) {
      return false;
    }
    assert(cur_node->pos_state == 2);
    assert(cur_node->isShared());

    // Acquire the lock in remote side
    auto remote_address = cur_node->remote_address;
    if (!try_get_remote_lock(remote_address)) {
      cur_node->writeUnlock();
      return false;
    }

    // read back remote node and compare whether the version changes
    auto remote_cur_node = raw_remote_read_by_sibling(remote_address);
    assert(reinterpret_cast<BTreeInner<Key> *>(remote_cur_node)
               ->check_consistent());

    // If this node is already full, we also require
    // the refresh from root
    if (reinterpret_cast<BTreeInner<Key> *>(remote_cur_node)->isFull()) {
      full_unlock(cur_node);
      refresh_cache = true;
      return false;
    }

    if (remote_cur_node->front_version == cur_node->front_version)
      return true;

    refresh_cache = true;
    // Refresh fromt the root
    full_unlock(cur_node);
    return false;
  }

  void load_newest_root(uint64_t versionNode, bool &needRestart) {
    auto remote_root = get_remote_root();
    auto cur_root = get_global_address(root);
    if ((remote_root.val != cur_root.val) ||
        (cache.search_in_cache(root) == nullptr)) {
      auto remote_root_node = opt_remote_read(remote_root);
      if (remote_root_node == nullptr)
        return;

      super_root_->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) {
        return;
      }

      needRestart = true;
      assert(cur_root == get_global_address(root));
      // Load the lock from remote to see whether it is locked
      if (test_remote_lock_set(root_lock_ptr_)) {
        super_root_->writeUnlock();
        return;
      }

      auto remote_check_root = get_remote_root();
      if (remote_root.val != remote_check_root.val) {
        super_root_->writeUnlock();
        return;
      }

      assert(cache.fit_limits(remote_root_node));

      cache.cache_insert(remote_root, remote_root_node, super_root_);
      auto old_mem_root =
          reinterpret_cast<NodeBase *>(get_memory_address(root));
      if (old_mem_root != nullptr) {
        old_mem_root->parent_ptr = nullptr;
      }

      root = remote_root;
      auto mem_root = reinterpret_cast<NodeBase *>(get_memory_address(root));
      assert(mem_root != nullptr);
      mem_root->parent_ptr = super_root_;
      super_root_->children[0] = root;
      super_root_->set_bitmap(0);
      height = mem_root->level + 1;
      std::cout << "Fetched new height = " << height << std::endl;
      mem_root->pos_state = 2;
      mem_root->writeUnlock();
      super_root_->writeUnlock();
      return;
    }
  }

  void new_swizzling(GlobalAddress &global_addr, NodeBase *parent,
                     unsigned child_idx, NodeBase *child) {
    if (!check_parent_child_info(parent, child)) {
      std::cout << "Gloobal addr = " << global_addr << std::endl;
      auto new_child = raw_remote_read(child->remote_address);
      assert(check_parent_child_info(parent, child));
    }
    child->parent_ptr = parent;
    child->pos_state = 2;
    global_addr.val = reinterpret_cast<uint64_t>(child) | swizzle_tag;
    auto inner_parent = reinterpret_cast<BTreeInner<Key> *>(parent);
    inner_parent->set_bitmap(child_idx);
    assert(inner_parent->level != 255);
    assert(check_parent_child_info(parent, child));
  }

  // Integrate pushdown and admission control here
  NodeBase *new_get_mem_node(GlobalAddress &global_addr, NodeBase *parent,
                             unsigned child_idx, uint64_t versionNode,
                             bool &restart, bool &refresh, bool IO_enable) {
    GlobalAddress node = global_addr;
    if (node.val & swizzle_tag) {
      return reinterpret_cast<NodeBase *>(node.val & swizzle_hide);
    }

    cache.opportunistic_sample();
    parent->upgradeToIOLockOrRestart(versionNode, restart);
    if (restart) {
      return nullptr;
    }

    GlobalAddress snapshot = global_addr;
    auto page = get_memory_address(snapshot);
    if (page) {
      parent->IOUnlock();
      return reinterpret_cast<NodeBase *>(page);
    }

    auto target_node =
        cache.cache_get(node, parent, child_idx, restart, refresh, IO_enable);
    if (target_node != nullptr) {
      new_swizzling(global_addr, parent, child_idx, target_node);
      target_node->pos_state = 2;
      target_node->writeUnlock();
    }

    // If IO is not supported, and IO flag is successfully inserteed
    if (!restart && target_node == nullptr) {
      assert(IO_enable == false);
      return target_node;
    }

    parent->IOUnlock();
    return target_node;
  }

  NodeBase *new_get_mem_node_without_lock(GlobalAddress &global_addr,
                                          NodeBase *parent, unsigned child_idx,
                                          bool &restart, bool &refresh,
                                          bool IO_enable) {
    GlobalAddress node = global_addr;
    if (node.val & swizzle_tag) {
      return reinterpret_cast<NodeBase *>(node.val & swizzle_hide);
    }

    cache.fill_local_page_set();
    auto target_node =
        cache.cache_get(node, parent, child_idx, restart, refresh, IO_enable);
    if (target_node != nullptr) {
      new_swizzling(global_addr, parent, child_idx, target_node);
      target_node->pos_state = 2;
      target_node->writeUnlock();
    }
    return target_node;
  }

  // Concurrent implementation of insert algorithm
  // Add optimistic lock coupling to the following insert algorithm
  //   bool insert(Key k, Value v) {
  //     int restartCount = 0;
  //     cache_allocator::Protect();
  //     bool verbose = false;
  //   restart:
  //     if (restartCount++)
  //       yield(restartCount);

  //     BTreeInner<Key> *parent = super_root_;
  //     bool needRestart = false;
  //     bool refresh = false;
  //     uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
  //     if (needRestart) {
  //       goto restart;
  //     }

  //     NodeBase *cur_node = cache.search_in_cache(root);
  //     if (cur_node == nullptr) {
  //       // Load newest root
  //       load_newest_root(versionParent, needRestart);
  //       goto restart;
  //     }

  //     uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
  //     if (needRestart) {
  //       goto restart;
  //     }

  //     while (cur_node->type == PageType::BTreeInner) {
  //       auto inner = static_cast<BTreeInner<Key> *>(cur_node);
  //       // SMO logic with megaNode
  //       // FIXME(BT): Not consider the root is not shared (single compute
  //       node) if (inner->isFull() && (!inner->isShared())) {
  //         // Need to differentiate shared node and non-shared node
  //         assert(parent != super_root_);
  //         if (parent->isShared()) {
  //           // Global Lock
  //           bool lock_success =
  //               verify_and_acquire_lock(parent, versionParent, refresh);
  //           if (!lock_success) {
  //             if (refresh) {
  //               new_refresh_from_root(k);
  //             }
  //             goto restart;
  //           }
  //         } else {
  //           // Normal lock
  //           parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
  //           if (needRestart) {
  //             goto restart;
  //           }
  //         }

  //         cur_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
  //         if (needRestart) {
  //           if (parent->isShared()) {
  //             full_unlock(parent);
  //           } else {
  //             parent->writeUnlock();
  //           }
  //           goto restart;
  //         }

  //         Key sep;
  //         GlobalAddress newInner;

  // // We don't do the online migration between memory nodes
  // #ifdef FINE_GRAINED
  //         newInner = allocate_node();
  // #else
  //         if (cur_node->level <= megaLevel - 1) {
  //           newInner = allocate_node(cur_node->remote_address.nodeID);
  //         } else {
  //           // Allocate in other memory nodes
  //           thread_local uint32_t next_node_id = 0;
  //           // newInner = allocate_node(cur_node->remote_address.nodeID + 1);
  //           newInner = allocate_node(next_node_id);
  //           ++next_node_id;
  //         }
  // #endif

  //         auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
  //         auto new_mem =
  //             new (page_buffer) BTreeInner<Key>(inner->level, newInner);
  //         // No write back for the private inner nodes
  //         inner->split(sep, new_mem);
  //         cache.cache_insert(newInner, new_mem, parent);
  //         auto mem_new_inner = cache.search_in_cache(newInner);
  //         assert(mem_new_inner != nullptr);
  //         assert(mem_new_inner->isLocked());
  //         mem_new_inner->pos_state = 2;
  //         update_all_parent_ptr(mem_new_inner);
  //         assert(parent != super_root_);
  //         parent->insert(sep, newInner);

  //         mem_new_inner->writeUnlock();
  //         cur_node->writeUnlock();
  //         if (parent->isShared()) {
  //           remote_write_unswizzle(parent);
  //           full_unlock(parent);
  //         } else {
  //           parent->writeUnlock();
  //         }

  //         // Unlock and restart
  //         goto restart;
  //       }

  //       parent->readUnlockOrRestart(versionParent, needRestart);
  //       if (needRestart) {
  //         goto restart;
  //       }

  //       parent = inner;
  //       versionParent = versionNode;
  //       // Got the next child to go
  //       auto idx = inner->lowerBound(k);
  //       cur_node = new_get_mem_node(inner->children[idx], inner, idx,
  //       versionNode,
  //                                   needRestart, refresh, true);
  //       if (needRestart) {
  //         if (refresh) {
  //           new_refresh_from_root(k);
  //         }
  //         goto restart;
  //       }

  //       assert(cur_node != nullptr);

  //       versionNode = cur_node->readLockOrRestart(needRestart);
  //       if (needRestart) {
  //         goto restart;
  //       }
  //     }

  //     // Access the leaf node
  //     BTreeLeaf<Key, Value> *leaf =
  //         static_cast<BTreeLeaf<Key, Value> *>(cur_node);

  //     leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
  //     if (needRestart) {
  //       goto restart;
  //     }

  //     if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
  //       leaf->writeUnlock();
  //       new_refresh_from_root(k);
  //       goto restart;
  //     }

  //     bool ret;
  //     if (leaf->count == leaf->maxEntries) {
  //       assert(parent != super_root_);
  //       if (parent->isShared()) {
  //         // Global Lock
  //         bool refresh_cache = false;
  //         bool lock_success =
  //             verify_and_acquire_lock(parent, versionParent, refresh_cache);
  //         if (!lock_success) {
  //           leaf->writeUnlock();
  //           if (refresh_cache) {
  //             new_refresh_from_root(k);
  //           }
  //           goto restart;
  //         }
  //       } else {
  //         parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
  //         if (needRestart) {
  //           leaf->writeUnlock();
  //           goto restart;
  //         }
  //       }

  //       //  Do the leaf split
  //       Key sep;
  // #ifdef FINE_GRAINED
  //       auto newLeaf = allocate_node();
  // #else
  //       auto newLeaf = allocate_node(cur_node->remote_address.nodeID);
  // #endif
  //       auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
  //       auto new_mem = new (page_buffer) BTreeLeaf<Key, Value>(newLeaf);
  //       leaf->split(sep, new_mem, newLeaf);
  //       remote_write(newLeaf, new_mem, true, true);

  //       parent->insert(sep, newLeaf);
  //       leaf->writeUnlock();
  //       if (parent->isShared()) {
  //         remote_write_unswizzle(parent);
  //         full_unlock(parent);
  //       } else {
  //         parent->writeUnlock();
  //       }
  //       goto restart;
  //     } else {
  //       parent->readUnlockOrRestart(versionParent, needRestart);
  //       if (needRestart) {
  //         leaf->writeUnlock();
  //         goto restart;
  //       }

  //       ret = leaf->insert(k, v);
  //       if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
  //         std::cout << "The key = " << k << std::endl;
  //         std::cout << "The leaf min limit = " << leaf->min_limit_ <<
  //         std::endl; std::cout << "The leaf max limit = " << leaf->max_limit_
  //         << std::endl;
  //       }
  //       assert(rangeValid(k, leaf->min_limit_, leaf->max_limit_));
  //       leaf->writeUnlock();
  //     }

  //     cache_allocator::Unprotect();
  //     return ret;
  //   }

  // Concurrent implementation of insert algorithm
  // Add optimistic lock coupling to the following insert algorithm
  bool insert(Key k, Value v) {
    int restartCount = 0;
    bool verbose = false;
    bool IO_enable = false;
  restart:
    if (restartCount++)
      yield(restartCount);

    BTreeInner<Key> *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    NodeBase *cur_node = cache.search_in_cache(root);
    if (cur_node == nullptr) {
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (cur_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(cur_node);
      // SMO logic with megaNode
      // FIXME(BT): Not consider the root is not shared (single compute node)
      if (inner->isFull() && (!inner->isShared())) {
        // Need to differentiate shared node and non-shared node
        assert(parent != super_root_);
        if (parent->isShared()) {
          // Global Lock
          bool lock_success =
              verify_and_acquire_lock(parent, versionParent, refresh);
          if (!lock_success) {
            if (refresh) {
              new_refresh_from_root(k);
            }
            goto restart;
          }
        } else {
          // Normal lock
          parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
          if (needRestart) {
            goto restart;
          }
        }

        cur_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
        if (needRestart) {
          if (parent->isShared()) {
            full_unlock(parent);
          } else {
            parent->writeUnlock();
          }
          goto restart;
        }

        Key sep;
        GlobalAddress newInner;

// We don't do the online migration between memory nodes
#ifdef FINE_GRAINED
        newInner = allocate_node();
#else
        if (cur_node->level <= megaLevel - 1) {
          newInner = allocate_node(cur_node->remote_address.nodeID);
        } else {
          // Allocate in other memory nodes
          thread_local uint32_t next_node_id = 0;
          // newInner = allocate_node(cur_node->remote_address.nodeID + 1);
          newInner = allocate_node(next_node_id);
          ++next_node_id;
        }
#endif

        auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
        auto new_mem =
            new (page_buffer) BTreeInner<Key>(inner->level, newInner);
        // No write back for the private inner nodes
        inner->split(sep, new_mem);
        cache.cache_insert(newInner, new_mem, parent);
        auto mem_new_inner = cache.search_in_cache(newInner);
        assert(mem_new_inner != nullptr);
        assert(mem_new_inner->isLocked());
        mem_new_inner->pos_state = 2;
        update_all_parent_ptr(mem_new_inner);
        assert(parent != super_root_);
        parent->insert(sep, newInner);

        mem_new_inner->writeUnlock();
        cur_node->writeUnlock();
        if (parent->isShared()) {
          remote_write_unswizzle(parent);
          full_unlock(parent);
        } else {
          parent->writeUnlock();
        }

        // Unlock and restart
        goto restart;
      }

      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, IO_enable);
      if (needRestart) {
        if (refresh) {
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // assert(cur_node != nullptr);
      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        assert(IO_enable == false);
        int remote_flag = 0;
        if (inner->level == 1) {
          // Admission control is needed
          bool insert_success = false;
          remote_flag = cache.cold_to_hot_with_admission(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, insert_success, RPC_type::INSERT);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return insert_success;
          }
        } else if (inner->level <= megaLevel && !(inner->shared)) {
          // RPC is probably needed
          bool insert_success = false;
          remote_flag = cache.cold_to_hot_with_rpc(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, insert_success, RPC_type::INSERT);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return insert_success;
          }
        } else {
          // Upper than subtree; then directly load from remote
          remote_flag = cache.cold_to_hot(inner->children[idx],
                                          reinterpret_cast<void **>(&cur_node),
                                          inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          new_swizzling(inner->children[idx], inner, idx, cur_node);
          cur_node->pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          IO_enable = true; // This means we do not do admission control and
                            // pushdown anymore
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    // Access the leaf node
    BTreeLeaf<Key, Value> *leaf =
        static_cast<BTreeLeaf<Key, Value> *>(cur_node);

    leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
      leaf->writeUnlock();
      new_refresh_from_root(k);
      goto restart;
    }

    bool ret;
    if (leaf->count == leaf->maxEntries) {
      assert(parent != super_root_);
      if (parent->isShared()) {
        // Global Lock
        bool refresh_cache = false;
        bool lock_success =
            verify_and_acquire_lock(parent, versionParent, refresh_cache);
        if (!lock_success) {
          leaf->writeUnlock();
          if (refresh_cache) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      } else {
        parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
        if (needRestart) {
          leaf->writeUnlock();
          goto restart;
        }
      }

      //  Do the leaf split
      Key sep;
#ifdef FINE_GRAINED
      auto newLeaf = allocate_node();
#else
      auto newLeaf = allocate_node(cur_node->remote_address.nodeID);
#endif
      auto page_buffer = (dsm_->get_rbuf(0)).get_sibling_buffer();
      auto new_mem = new (page_buffer) BTreeLeaf<Key, Value>(newLeaf);
      leaf->split(sep, new_mem, newLeaf);
      remote_write(newLeaf, new_mem, true, true);

      parent->insert(sep, newLeaf);
      leaf->writeUnlock();
      if (parent->isShared()) {
        remote_write_unswizzle(parent);
        full_unlock(parent);
      } else {
        parent->writeUnlock();
      }
      goto restart;
    } else {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        leaf->writeUnlock();
        goto restart;
      }

      ret = leaf->insert(k, v);
      if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
        std::cout << "The key = " << k << std::endl;
        std::cout << "The leaf min limit = " << leaf->min_limit_ << std::endl;
        std::cout << "The leaf max limit = " << leaf->max_limit_ << std::endl;
      }
      assert(rangeValid(k, leaf->min_limit_, leaf->max_limit_));
      leaf->writeUnlock();
    }

    return ret;
  }

  bool rangeValid(Key k, Key cur_min, Key cur_max) {
    if (std::numeric_limits<Key>::min() == k && k == cur_min)
      return true;
    if (k <= cur_min || k > cur_max)
      return false;
    return true;
  }

  bool lookup(Key k, Value &result) {
    int restartCount = 0;
  restart:
    if (restartCount++)
      yield(restartCount);
    BTreeInner<Key> *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart)
      goto restart;

    NodeBase *cur_node = cache.search_in_cache(root);
    if (cur_node == nullptr) {
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (cur_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(cur_node);
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, false);
      if (needRestart) {
        if (refresh) {
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        int remote_flag = 0;
        if (inner->level == 1) {
          // Admission control is needed
          bool lookup_success = false;
          remote_flag = cache.cold_to_hot_with_admission(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, result, lookup_success, RPC_type::LOOKUP);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return lookup_success;
          }
        } else if (inner->level <= megaLevel && !(inner->shared)) {
          // RPC is probably needed
          bool lookup_success = false;
          remote_flag = cache.cold_to_hot_with_rpc(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, result, lookup_success, RPC_type::LOOKUP);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return lookup_success;
          }
        } else {
          // Upper than subtree; then directly load from remote
          remote_flag = cache.cold_to_hot(inner->children[idx],
                                          reinterpret_cast<void **>(&cur_node),
                                          inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          new_swizzling(inner->children[idx], inner, idx, cur_node);
          cur_node->pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart)
        goto restart;
    }

    // Access the leaf node
    BTreeLeaf<Key, Value> *leaf =
        static_cast<BTreeLeaf<Key, Value> *>(cur_node);
    if (!leaf->rangeValid(k)) {
      new_refresh_from_root(k);
      goto restart;
    }

    // assert(leaf->rangeValid(k) == true);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }

    auto backup_min = leaf->min_limit_;
    auto backup_max = leaf->max_limit_;

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    cur_node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    assert(rangeValid(k, backup_min, backup_max));
    return success;
  }

  // Return the final scaned item
  int range_scan(Key k, uint32_t num, std::pair<Key, Value> *&kv_buffer) {
    int restartCount = 0;
    int cur_num = 0;
  restart:
    if (restartCount++)
      yield(restartCount);
    BTreeInner<Key> *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart)
      goto restart;

    NodeBase *cur_node = cache.search_in_cache(root);
    if (cur_node == nullptr) {
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (cur_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(cur_node);
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, false);
      if (needRestart) {
        if (refresh) {
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        int remote_flag = 0;
        if (inner->level == 1) {
          // Admission control is needed
          bool lookup_success = false;
          int scan_num = num - cur_num;
          Key max_key;
          remote_flag = cache.cold_to_hot_with_admission_for_scan(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, kv_buffer, scan_num, max_key);
          if (remote_flag == 1) {
            inner->IOUnlock();
            // Compute the rest
            cur_num += scan_num;
            if (max_key == std::numeric_limits<Key>::max() ||
                (max_key == (right_bound_ - 1)) || cur_num == num) {
              return cur_num;
            }
            k = max_key + 1;
            goto restart;
          }
        } else {
          // Upper than subtree; then directly load from remote
          remote_flag = cache.cold_to_hot(inner->children[idx],
                                          reinterpret_cast<void **>(&cur_node),
                                          inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          new_swizzling(inner->children[idx], inner, idx, cur_node);
          cur_node->pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart)
        goto restart;
    }

    // Access the leaf node
    BTreeLeaf<Key, Value> *leaf =
        static_cast<BTreeLeaf<Key, Value> *>(cur_node);
    if (!leaf->rangeValid(k)) {
      new_refresh_from_root(k);
      goto restart;
    }

    auto ret_num = leaf->range_scan(k, num - cur_num, kv_buffer);
    auto backup_min = leaf->min_limit_;
    auto backup_max = leaf->max_limit_;
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    cur_node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    assert(rangeValid(k, backup_min, backup_max));
    cur_num += ret_num;
    if ((backup_max == std::numeric_limits<Key>::max()) ||
        (backup_max == right_bound_ - 1) || (cur_num == num)) {
      return cur_num;
    }
    k = backup_max + 1;
    goto restart;
  }

  bool update(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++)
      yield(restartCount);

    BTreeInner<Key> *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart)
      goto restart;

    NodeBase *cur_node = cache.search_in_cache(root);
    if (cur_node == nullptr) {
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (cur_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(cur_node);
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, false);
      if (needRestart) {
        if (refresh) {
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        int remote_flag = 0;
        if (inner->level == 1) {
          // Admission control is needed
          bool update_success = false;
          remote_flag = cache.cold_to_hot_with_admission(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, update_success, RPC_type::UPDATE);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return update_success;
          }
        } else if (inner->level <= megaLevel && !(inner->shared)) {
          // RPC is probably needed
          bool update_success = false;
          remote_flag = cache.cold_to_hot_with_rpc(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, update_success, RPC_type::UPDATE);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return update_success;
          }
        } else {
          // Upper than subtree; then directly load from remote
          remote_flag = cache.cold_to_hot(inner->children[idx],
                                          reinterpret_cast<void **>(&cur_node),
                                          inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          new_swizzling(inner->children[idx], inner, idx, cur_node);
          cur_node->pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart)
        goto restart;
    }

    // Access the leaf node
    BTreeLeaf<Key, Value> *leaf =
        static_cast<BTreeLeaf<Key, Value> *>(cur_node);

    leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
      leaf->writeUnlock();
      new_refresh_from_root(k);
      goto restart;
    }

    parent->readUnlockOrRestart(versionParent, needRestart);
    if (needRestart) {
      leaf->writeUnlock();
      goto restart;
    }

    auto ret = leaf->update(k, v);
    leaf->writeUnlock();
    return ret;
  }

  // bool update(Key k, Value v) {
  //   int restartCount = 0;
  // restart:
  //   if (restartCount++)
  //     yield(restartCount);

  //   BTreeInner<Key> *parent = super_root_;
  //   bool needRestart = false;
  //   bool refresh = false;
  //   uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
  //   if (needRestart)
  //     goto restart;

  //   NodeBase *cur_node = cache.search_in_cache(root);
  //   if (cur_node == nullptr) {
  //     // Load newest root
  //     load_newest_root(versionParent, needRestart);
  //     goto restart;
  //   }

  //   uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
  //   if (needRestart) {
  //     goto restart;
  //   }

  //   while (cur_node->type == PageType::BTreeInner) {
  //     auto inner = static_cast<BTreeInner<Key> *>(cur_node);
  //     parent->readUnlockOrRestart(versionParent, needRestart);
  //     if (needRestart) {
  //       goto restart;
  //     }

  //     parent = inner;
  //     versionParent = versionNode;
  //     // Got the next child to go
  //     auto idx = inner->lowerBound(k);
  //     cur_node = new_get_mem_node(inner->children[idx], inner, idx,
  //     versionNode,
  //                                 needRestart, refresh, true);
  //     if (needRestart) {
  //       if (refresh) {
  //         new_refresh_from_root(k);
  //       }
  //       goto restart;
  //     }
  //     assert(cur_node != nullptr);

  //     // Get the version of next node
  //     versionNode = cur_node->readLockOrRestart(needRestart);
  //     if (needRestart)
  //       goto restart;
  //   }

  //   // Access the leaf node
  //   BTreeLeaf<Key, Value> *leaf =
  //       static_cast<BTreeLeaf<Key, Value> *>(cur_node);

  //   leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
  //   if (needRestart) {
  //     goto restart;
  //   }

  //   if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
  //     leaf->writeUnlock();
  //     new_refresh_from_root(k);
  //     goto restart;
  //   }

  //   parent->readUnlockOrRestart(versionParent, needRestart);
  //   if (needRestart) {
  //     leaf->writeUnlock();
  //     goto restart;
  //   }

  //   auto ret = leaf->update(k, v);
  //   leaf->writeUnlock();
  //   return ret;
  // }

  bool remove(Key k) {
    int restartCount = 0;
  restart:
    if (restartCount++)
      yield(restartCount);

    BTreeInner<Key> *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart)
      goto restart;

    NodeBase *cur_node = cache.search_in_cache(root);
    if (cur_node == nullptr) {
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (cur_node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(cur_node);
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      cur_node = new_get_mem_node(inner->children[idx], inner, idx, versionNode,
                                  needRestart, refresh, false);
      if (needRestart) {
        if (refresh) {
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        int remote_flag = 0;
        if (inner->level == 1) {
          // Admission control is needed
          bool remove_success = false;
          Value v;
          remote_flag = cache.cold_to_hot_with_admission(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, remove_success, RPC_type::DELETE);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return remove_success;
          }
        } else if (inner->level <= megaLevel && !(inner->shared)) {
          // RPC is probably needed
          bool remove_success = false;
          Value v;
          remote_flag = cache.cold_to_hot_with_rpc(
              inner->children[idx], reinterpret_cast<void **>(&cur_node), inner,
              idx, refresh, k, v, remove_success, RPC_type::DELETE);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return remove_success;
          }
        } else {
          // Upper than subtree; then directly load from remote
          remote_flag = cache.cold_to_hot(inner->children[idx],
                                          reinterpret_cast<void **>(&cur_node),
                                          inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          new_swizzling(inner->children[idx], inner, idx, cur_node);
          cur_node->pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart)
        goto restart;
    }

    // Access the leaf node
    BTreeLeaf<Key, Value> *leaf =
        static_cast<BTreeLeaf<Key, Value> *>(cur_node);

    leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    if (!rangeValid(k, leaf->min_limit_, leaf->max_limit_)) {
      leaf->writeUnlock();
      new_refresh_from_root(k);
      goto restart;
    }

    parent->readUnlockOrRestart(versionParent, needRestart);
    if (needRestart) {
      leaf->writeUnlock();
      goto restart;
    }

    auto ret = leaf->remove(k);
    leaf->writeUnlock();
    return ret;
  }

  // For debug purpose; to know whether the tree is distributed in meganode way
  void validate() {
    flush_all();
    std::vector<uint64_t> node_num(128, 0);
    bool ret = recursive_validate(get_global_address(root),
                                  GlobalAddress::Null(), false, node_num);
    if (ret == false) {
      std::cout << "-----The tree fail to be distributed in MegaNode way-----"
                << std::endl;
    } else {
      std::cout << "-----The tree has been distributed in MegaNode way-----"
                << std::endl;
    }

    for (int i = 0; i < 128; ++i) {
      if (node_num[i] != 0) {
        std::cout << "#BTree nodes in memory node " << i << " = " << node_num[i]
                  << std::endl;
      }
    }

    // Print the state of buffer pool
    std::cout << "Buffer pool state = " << cache.state << std::endl;

    // Validate whether the #pages in page tables equals to cache.capacity
    auto page_table = cache.page_table_;
    uint64_t index_page_num = 0;
    for (uint64_t i = 0; i < page_table->bucket_num_; ++i) {
      auto cur_bucket = page_table->table_ + i;
      while (cur_bucket) {
        index_page_num += cur_bucket->count_;
        cur_bucket = cur_bucket->next_;
      }
    }

    // Validate whether the page table already indexes all pages
    std::cout << "IN Page Table, it has " << index_page_num
              << " pages; The buffer pool has " << cache.capacity << " pages"
              << std::endl;

    // Scan all pages in buffer pool to ensure they are in page table and
    // cooling page is in hash table
    uint64_t page_num = cache_allocator::instance_->page_num_;
    uint64_t start = cache_allocator::instance_->base_address;
    uint64_t remote_state = 0;
    uint64_t cooling_state = 0;
    uint64_t wrong_cooling_hash_state = 0;
    uint64_t wrong_cooling_page_state = 0;
    uint64_t hot_state = 0;
    uint64_t wrong_hot_page_state = 0;
    uint64_t free_state = 0;
    uint64_t wrong_free_page_state = 0;
    uint64_t other_state = 0;
    uint64_t shared_node = 0;
    for (uint64_t i = 0; i < page_num; ++i) {
      auto cur_page = reinterpret_cast<NodeBase *>(start + i * pageSize);
      if (cur_page->isShared()) {
        ++shared_node;
      }
      if (cur_page->pos_state == 0) {
        remote_state++;
      } else if (cur_page->pos_state == 1) {
        cooling_state++;
        // Check wether this page is in hash table
        auto flag = cache.hash_table_->check_existence(
            cur_page->remote_address, reinterpret_cast<void *>(cur_page));
        if (!flag)
          ++wrong_cooling_hash_state;
        auto ret_page =
            cache.page_table_->get_with_lock(cur_page->remote_address);
        if (ret_page != reinterpret_cast<void *>(cur_page) &&
            cur_page->obsolete == false) {
          ++wrong_cooling_page_state;
        }
      } else if (cur_page->pos_state == 2) {
        // Check whether this page is in page table
        auto ret_page =
            cache.page_table_->get_with_lock(cur_page->remote_address);
        if (ret_page != reinterpret_cast<void *>(cur_page) &&
            cur_page->obsolete == false) {
          ++wrong_hot_page_state;
        }
        hot_state++;
      } else if (cur_page->pos_state == 3) {
        auto ret_page =
            cache.page_table_->get_with_lock(cur_page->remote_address);
        if (ret_page == reinterpret_cast<void *>(cur_page)) {
          ++wrong_free_page_state;
        }
        free_state++;
      } else {
        other_state++;
      }
    }

    if ((hot_state + cooling_state) == index_page_num) {
      std::cout << "All Hot and Cooling pages are indexed by the page table"
                << std::endl;
    } else {
      std::cout << "Wrong: not all pages are indexed by the page table"
                << std::endl;
    }

    if ((hot_state + cooling_state + free_state) == cache.capacity) {
      std::cout << "All pages are found with correct state" << std::endl;
    } else {
      std::cout << "Some pages are not in correct state" << std::endl;
    }

    std::cout << "--------------------------------" << std::endl;
    std::cout << "#Shared node in buffer = " << shared_node << std::endl;
    std::cout << "#Remote state = " << remote_state << std::endl;
    std::cout << "#Cooling state = " << cooling_state << std::endl;
    std::cout << "#Hot state = " << hot_state << std::endl;
    std::cout << "#Free state = " << free_state << std::endl;
    std::cout << "#Other state = " << other_state << std::endl;
    std::cout << "Shared ratio = "
              << shared_node / static_cast<double>(page_num) << std::endl;
    std::cout << "Cooling ratio = "
              << cooling_state / static_cast<double>(page_num) << std::endl;
    std::cout << "Hot ratio = " << hot_state / static_cast<double>(page_num)
              << std::endl;
    std::cout << "Free ratio = " << free_state / static_cast<double>(page_num)
              << std::endl;
    std::cout << "--------------------------------" << std::endl;
    std::cout << "#wrong cooling hash state = " << wrong_cooling_hash_state
              << std::endl;
    std::cout << "#wrong cooling page state = " << wrong_cooling_page_state
              << std::endl;
    std::cout << "#wrong hot page state = " << wrong_hot_page_state
              << std::endl;
    std::cout << "#wrong free page state = " << wrong_free_page_state
              << std::endl;
    std::cout << "--------------------------------" << std::endl;
  }

  bool recursive_validate(GlobalAddress cur_node, GlobalAddress parent,
                          bool validate_parent,
                          std::vector<uint64_t> &node_num) {
    node_num[cur_node.nodeID]++;
    assert(cur_node != GlobalAddress::Null());
    if (validate_parent && cur_node.nodeID != parent.nodeID) {
      std::cout << "Fail from this one: cur node ID = " << cur_node.nodeID
                << "; parent node ID = " << parent.nodeID << std::endl;
      return false;
    }

    // in-memory version of cur_node
    auto mem_node = reinterpret_cast<NodeBase *>(malloc(pageSize));
    NodeBase *page_node = raw_remote_read(cur_node);
    memcpy(reinterpret_cast<void *>(mem_node),
           reinterpret_cast<void *>(page_node), pageSize);

    if (mem_node->type == PageType::BTreeLeaf) {
      free(mem_node);
      return true;
    }

    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    int num = inner->count;
    for (int i = 0; i <= num; ++i) {
      auto next_node = get_global_address(inner->children[i]);
      bool validate = true;
      if (inner->level % megaLevel == 0)
        validate = false;

      bool ret = recursive_validate(next_node, cur_node, validate, node_num);
      if (ret == false) {
        std::cout << "Recursive fail: cur node ID = " << cur_node.nodeID
                  << "; parent node ID = " << parent.nodeID
                  << " ; cur node level = " << inner->level << std::endl;
        free(mem_node);
        return false;
      }
    }

    free(mem_node);
    return true;
  }

  // Flush the B+-tree cache
  void flush_all() {
    std::cout << "Flush all pages in buffer pool" << std::endl;
    cache.flush_all();
  }

  void reset_buffer_pool(bool flush_dirty) { cache.reset(flush_dirty); }

  void statistic_in_buffer() { cache.statistic_in_buffer(); }

  void set_rpc_ratio(double ratio) { cache.set_rpc_ratio(ratio); }

  void set_admission_ratio(double ratio) { cache.set_admission_ratio(ratio); }

  double get_rpc_ratio() { return cache.get_rpc_ratio(); }

  void clear_statistic() {}
};

} // namespace cachepush