#pragma once

#include "../DSM.h"
#include "btree_node.h"
namespace cachepush {

// #define RDMATIMER 1

DSM *global_dsm_ = nullptr; // Distributed shared memory
// uint64_t read_counter = 0;
// uint64_t write_counter = 0;

/*----------------------------------*/
/***** One-sided node operation *****/
/*----------------------------------*/

// Return nullptr means there is global conflict in remote side
NodeBase *opt_remote_read(GlobalAddress global_node) {
  NodeBase *mem_node = nullptr;
  // 1. Read the lock & version
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
  global_dsm_->read_sync(page_buffer, global_node, 16, nullptr);
  mem_node = reinterpret_cast<NodeBase *>(page_buffer);
  if (mem_node->typeVersionLockObsolete != 0)
    return nullptr;
  uint64_t org_version = mem_node->front_version;

  // 2. Read the node content
  global_dsm_->read_sync(page_buffer, global_node, pageSize, nullptr);
  mem_node = reinterpret_cast<NodeBase *>(page_buffer);

  // 3. version verification
  auto new_page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
  global_dsm_->read_sync(new_page_buffer, global_node, 16, nullptr);
  NodeBase *new_mem_node = reinterpret_cast<NodeBase *>(new_page_buffer);
  if ((new_mem_node->typeVersionLockObsolete != 0) ||
      (new_mem_node->front_version != org_version)) {
    return nullptr;
  }

  if (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    assert(inner->check_consistent());
  } else {
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
    assert(leaf->check_consistent());
  }

  // assert(mem_node->front_version == mem_node->back_version);
  return mem_node;
}

// Consistent read using optimistic locking (From Carsten's SIGMOD 23')
NodeBase *remote_consistent_read(GlobalAddress global_node) {
  NodeBase *mem_node = nullptr;
  while (true) {
    mem_node = opt_remote_read(global_node);
    if (mem_node != nullptr)
      break;
  }
  return mem_node;
}

char *raw_remote_read(GlobalAddress global_node, size_t read_size) {
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
  global_dsm_->read_sync(page_buffer, global_node, read_size, nullptr);
  return page_buffer;
}

NodeBase *raw_remote_read(GlobalAddress global_node) {
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
  global_dsm_->read_sync(page_buffer, global_node, pageSize, nullptr);
  auto mem_node = reinterpret_cast<NodeBase *>(page_buffer);
  return mem_node;
}

NodeBase *raw_remote_read_by_sibling(GlobalAddress global_node) {
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_sibling_buffer();
  global_dsm_->read_sync(page_buffer, global_node, pageSize, nullptr);
  auto mem_node = reinterpret_cast<NodeBase *>(page_buffer);
  return mem_node;
}

void remote_write(GlobalAddress global_node, NodeBase *mem_node,
                  bool clear_lock = true, bool without_copy = false) {
  //++write_counter;
  char *page_buffer = nullptr;
  mem_node->dirty = false;
  if (without_copy) {
    page_buffer = reinterpret_cast<char *>(mem_node);
  } else {
    page_buffer = (global_dsm_->get_rbuf(0)).get_page_buffer();
    if (page_buffer != reinterpret_cast<char *>(mem_node)) {
      memcpy(page_buffer, mem_node, pageSize);
    }
  }

  auto flushed_page = reinterpret_cast<NodeBase *>(page_buffer);
  flushed_page->parent_ptr = nullptr;
  flushed_page->pos_state = 0;
  if (clear_lock)
    flushed_page->typeVersionLockObsolete = 0;

  if (flushed_page->remote_address != global_node) {
    std::cout << "Remote write page addr is not consistent!!!" << std::endl;
    while (true)
      ;
  }
#ifdef RDMATIMER
  thread_local uint64_t zero_counter = 0;
  thread_local uint64_t one_counter = 0;
  thread_local std::chrono::nanoseconds zero_total_time(0);
  thread_local std::chrono::nanoseconds one_total_time(0);
  auto start = std::chrono::high_resolution_clock::now();
#endif

  global_dsm_->write_sync(page_buffer, global_node, pageSize);

#ifdef RDMATIMER
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  if (global_node.nodeID == 0) {
    ++zero_counter;
    zero_total_time = zero_total_time + duration;
    if (zero_counter % 100000 == 0) {
      std::cout << "0 Write time comsumption = "
                << zero_total_time.count() / zero_counter << std::endl;
    }
  } else if (global_node.nodeID == 1) {
    ++one_counter;
    one_total_time = one_total_time + duration;
    if (one_counter % 100000 == 0) {
      std::cout << "1 Write time comsumption = "
                << one_total_time.count() / one_counter << std::endl;
    }
  }
#endif
}

NodeBase *opt_remote_read_by_sibling(GlobalAddress global_node) {
  NodeBase *mem_node = nullptr;
  // int count = 0;
  //  1. Read the lock & version
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_sibling_buffer();
  global_dsm_->read_sync(page_buffer, global_node, 16, nullptr);
  mem_node = reinterpret_cast<NodeBase *>(page_buffer);
  if (mem_node->typeVersionLockObsolete != 0)
    return nullptr;
  uint64_t org_version = mem_node->front_version;

  // 2. Read the node content
  global_dsm_->read_sync(page_buffer, global_node, pageSize, nullptr);
  mem_node = reinterpret_cast<NodeBase *>(page_buffer);

  // 3. version verification
  auto new_page_buffer = (global_dsm_->get_rbuf(0)).get_sibling_buffer();
  global_dsm_->read_sync(new_page_buffer, global_node, 16, nullptr);
  NodeBase *new_mem_node = reinterpret_cast<NodeBase *>(new_page_buffer);
  if ((new_mem_node->typeVersionLockObsolete != 0) ||
      (new_mem_node->front_version != org_version)) {
    return nullptr;
  }

  if (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    assert(inner->check_consistent());
  } else {
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
    assert(leaf->check_consistent());
  }

  // assert(mem_node->front_version == mem_node->back_version);
  return mem_node;
}

// Consistent read using optimistic locking (From Carsten's SIGMOD 23')
NodeBase *remote_consistent_read_by_sibling(GlobalAddress global_node) {
  NodeBase *mem_node = nullptr;
  while (true) {
    mem_node = opt_remote_read_by_sibling(global_node);
    if (mem_node != nullptr)
      break;
  }
  return mem_node;
}

void remote_write_by_sibling(GlobalAddress global_node, NodeBase *mem_node,
                             bool clear_lock = true) {
  //++write_counter;
  auto page_buffer = (global_dsm_->get_rbuf(0)).get_sibling_buffer();
  mem_node->dirty = false;
  if (page_buffer != reinterpret_cast<char *>(mem_node)) {
    memcpy(page_buffer, mem_node, pageSize);
  }
  auto flushed_page = reinterpret_cast<NodeBase *>(page_buffer);
  flushed_page->pos_state = 0;
  if (clear_lock)
    flushed_page->typeVersionLockObsolete = 0;
  global_dsm_->write_sync(page_buffer, global_node, pageSize);
}

void get_node_statistic() {
  std::cout << "NodeBase size = " << sizeof(NodeBase) << std::endl;
  std::cout << "BtreeLeaf size = " << sizeof(BTreeLeaf<Key, Value>)
            << std::endl;
  std::cout << "BTreeInner size = " << sizeof(BTreeInner<Key>) << std::endl;
}

uint64_t murmur64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53L;
  k ^= k >> 33;
  return k;
}

void update_all_parent_ptr(NodeBase *mem_node) {
  if (mem_node->type == PageType::BTreeLeaf) {
    return;
  }

  auto inner = reinterpret_cast<BTreeInner<Key> *>(mem_node);
  for (int i = 0; i < (inner->count + 1); ++i) {
    if (inner->children[i].val & swizzle_tag) {
      auto mem_child =
          reinterpret_cast<NodeBase *>(inner->children[i].val & swizzle_hide);
      mem_child->parent_ptr = mem_node;
    }
  }
}

// Without Bitmap
NodeBase *recursive_iterate_without_bitmap(NodeBase *mem_node,
                                           int &idx_in_parent) {
  // First test whether this is a mininode and randomly select which node to
  // choose
  bool needRestart = false;
  uint64_t versionNode = mem_node->readLockOrRestart(needRestart);
  if (needRestart)
    return nullptr;

  BTreeInner<Key> *parent = nullptr;
  uint64_t versionParent = 0;

  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart)
        return nullptr;
    }

    parent = inner;
    versionParent = versionNode;

    uint64_t hash_k = murmur64(reinterpret_cast<uint64_t>(mem_node));
    auto inner_count = inner->count;
    int start_idx = hash_k % (inner_count + 1);
    int next_child_idx;
    for (int i = 0; i < inner_count + 1; i++) {
      next_child_idx = (start_idx + i) % (inner_count + 1);
      auto node_val = inner->children[next_child_idx].val;
      if (node_val & swizzle_tag) {
        idx_in_parent = next_child_idx;
        mem_node = reinterpret_cast<NodeBase *>(node_val & swizzle_hide);
        break;
      }
    }

    if (mem_node == reinterpret_cast<NodeBase *>(parent)) {
      break;
    }

    versionNode = mem_node->readLockOrRestart(needRestart);
    if (needRestart)
      return nullptr;
  }

  mem_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
  if (needRestart)
    return nullptr;

  if (mem_node->pos_state != 2) {
    mem_node->writeUnlock();
    return nullptr;
  }

  if (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    for (int i = 0; i < inner->count + 1; ++i) {
      if (inner->children[i].val & swizzle_tag) {
        inner->writeUnlock();
        return nullptr;
      }
    }
  }

  return mem_node;
}

//- 1 means we don't know which idx points to this child NodeBase *
// This iteration is not protected by epoch
NodeBase *recursive_iterate(NodeBase *mem_node, int &idx_in_parent) {
  // First test whether this is a mininode and randomly select which node to
  // choose
  bool needRestart = false;
  uint64_t versionNode = mem_node->readLockOrRestart(needRestart);
  if (needRestart)
    return nullptr;

  BTreeInner<Key> *parent = nullptr;
  uint64_t versionParent = 0;

  while (mem_node->bitmap != 0) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart)
        return nullptr;
    }

    parent = inner;
    versionParent = versionNode;

    uint64_t hash_k = murmur64(reinterpret_cast<uint64_t>(mem_node));
    int idx = hash_k % (inner->count + 1);

    auto next_idx = inner->closest_set(idx);
    if (next_idx == -1)
      return nullptr;
    idx_in_parent = next_idx;

    auto next_node = inner->children[next_idx].val;
    mem_node = reinterpret_cast<NodeBase *>(next_node & swizzle_hide);

    inner->checkOrRestart(versionNode, needRestart);
    if (needRestart)
      return nullptr;

    assert((next_node & swizzle_tag) != 0);
    versionNode = mem_node->readLockOrRestart(needRestart);
    if (needRestart)
      return nullptr;
  }

  mem_node->upgradeToWriteLockOrRestart(versionNode, needRestart);
  if (needRestart)
    return nullptr;

  if ((mem_node->bitmap != 0) || (mem_node->pos_state != 2)) {
    mem_node->writeUnlock();
    return nullptr;
  }

  return mem_node;
}

void fully_unswizzle(NodeBase *mem_node) {
  if (mem_node->type == PageType::BTreeLeaf)
    return;

  auto inner = reinterpret_cast<BTreeInner<Key> *>(mem_node);
  for (int i = 0; i < inner->count + 1; ++i) {
    if (inner->children[i].val & swizzle_tag) {
      assert(inner->bitmap & (1ULL << i));
      auto child =
          reinterpret_cast<NodeBase *>(inner->children[i].val & swizzle_hide);
      inner->children[i] = child->remote_address;
    }
  }
  inner->bitmap = 0;
}

// check whether the min/max limits match
bool check_limit_match(NodeBase *parent, NodeBase *child,
                       bool using_swizzle = false, bool verbose = false) {
  auto inner = reinterpret_cast<BTreeInner<Key> *>(parent);
  int idx = -1;
  if (using_swizzle) {
    idx = inner->findIdx(reinterpret_cast<uint64_t>(child) | swizzle_tag);
  } else {
    idx = inner->findIdx(child->remote_address.val);
  }
  if (verbose)
    std::cout << "The idx = " << idx << std::endl;
  if (idx == -1)
    return false;

  Key min_limit = (idx == 0) ? inner->min_limit_ : inner->keys[idx - 1];
  Key max_limit = (idx == inner->count) ? inner->max_limit_ : inner->keys[idx];
  if (verbose) {
    std::cout << "Parent min limit = " << parent->min_limit_
              << ", max limit = " << parent->max_limit_ << std::endl;
    std::cout << "Parent level = " << static_cast<int>(parent->level)
              << std::endl;
    std::cout << "Min limit in parent = " << min_limit
              << ", max_limit in parent = " << max_limit << std::endl;
    std::cout << "Child level = " << static_cast<int>(child->level)
              << std::endl;
    std::cout << "child min limit = " << child->min_limit_
              << ", max limit = " << child->max_limit_ << std::endl;
  }
  if (min_limit == child->min_limit_ && max_limit == child->max_limit_) {
    return true;
  }
  return false;
}

bool new_check_limit_match(NodeBase *parent, NodeBase *child, unsigned idx) {
  auto inner = reinterpret_cast<BTreeInner<Key> *>(parent);
  Key min_limit = (idx == 0) ? inner->min_limit_ : inner->keys[idx - 1];
  Key max_limit = (idx == inner->count) ? inner->max_limit_ : inner->keys[idx];
  if (min_limit == child->min_limit_ && max_limit == child->max_limit_) {
    return true;
  }
  return false;
}

bool check_parent_child_info(NodeBase *parent, NodeBase *child) {
  if (parent->level == 255)
    return true;
  bool verbose = false;
  auto parent_level = static_cast<int>(parent->level);
  auto child_level = static_cast<int>(child->level);
  if (parent_level != child_level + 1) {
    verbose = true;
  }

  if (child->min_limit_ < parent->min_limit_ ||
      child->max_limit_ > parent->max_limit_) {
    verbose = true;
  }

  if (!(check_limit_match(parent, child, true) ||
        check_limit_match(parent, child, false))) {
    verbose = true;
  }

  if (verbose) {
    if (child->parent_ptr == parent) {
      std::cout << "Parent is child's parent" << std::endl;
    } else {
      std::cout << "Parent is not child's parent" << std::endl;
    }

    std::cout << "Parent level = " << parent_level << std::endl;
    std::cout << "Child level = " << child_level << std::endl;

    std::cout << "parent min limit = " << parent->min_limit_
              << "; parent max limit = " << parent->max_limit_ << std::endl;
    std::cout << "child min limit = " << child->min_limit_
              << "; child max limit = " << child->max_limit_ << std::endl;

    if (parent->type == PageType::BTreeLeaf) {
      std::cout << "The parent is a leaf node." << std::endl;
    } else if (parent->type == PageType::BTreeInner) {
      std::cout << "The parent is a inner node." << std::endl;
    }

    if (child->type == PageType::BTreeLeaf) {
      std::cout << "The child is a leaf node." << std::endl;
    } else if (child->type == PageType::BTreeInner) {
      std::cout << "The child is a inner node." << std::endl;
    }
    std::cout << "1. --------------------------------" << std::endl;
    check_limit_match(parent, child, true, true);
    std::cout << "2. --------------------------------" << std::endl;
    check_limit_match(parent, child, false, true);
  }

  return !verbose;
}

void check_global_conflict(BTreeInner<Key> *remote_cur_node,
                           uint64_t org_version, bool &needRestart) {
  if (remote_cur_node->typeVersionLockObsolete != 0) {
    needRestart = true;
  }

  if (org_version != remote_cur_node->front_version) {
    needRestart = true;
  }
}

} // namespace cachepush