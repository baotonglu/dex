#pragma once

#include "../GlobalAddress.h"
#include "btree_node.h"

namespace cachepush {

// -1 means lookup failure, 0 means next global_address_ptr, 1 means find value
// result, 2 means find nothing
int lookup(GlobalAddress root, uint64_t dsm_base, Key k, Value &v_result,
           GlobalAddress &g_result) {
  auto node_id = root.nodeID;
  auto node = root;
  NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    node = inner->children[inner->lowerBound(k)];
    if (node.nodeID != node_id) {
      g_result = node;
      return 0;
    } else {
      mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
    }
  }

  BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
  if (!leaf->rangeValid(k)) {
    return -1;
  }

  unsigned pos = leaf->lowerBound(k);
  int ret = 2;
  if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
    ret = 1;
    v_result = leaf->data[pos].second;
  }

  return ret;
}

// -1 means update failure because enterring wrong leaf node, 0 means failure
// because not enter into a leaf node, 1 means update value succeeds, 2 means
// update nothing
int update(GlobalAddress &root, uint64_t dsm_base, Key k, Value v_result) {
  auto node_id = root.nodeID;
  auto node = root;
  NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    node = inner->children[inner->lowerBound(k)];
    if (node.nodeID != node_id) {
      return 0;
    } else {
      mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
    }
  }

  BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
  if (!leaf->rangeValid(k)) {
    return -1;
  }

  unsigned pos = leaf->lowerBound(k);
  int ret = 2;
  if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
    ret = 1;
    leaf->data[pos].second = v_result;
    root = leaf->remote_address;
  }

  return ret;
}

// -1 means insert failure because enterring wrong leaf node (-1 also means it
// needs to SMO), 0 means failure because not enter into a leaf node, 1 means
// insert value succeeds, 2 means update a existing value;
int insert(GlobalAddress &root, uint64_t dsm_base, Key k, Value v_result) {
  auto node_id = root.nodeID;
  auto node = root;
  NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    node = inner->children[inner->lowerBound(k)];
    if (node.nodeID != node_id) {
      return 0;
    } else {
      mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
    }
  }

  BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
  if (!leaf->rangeValid(k) || (leaf->count == leaf->maxEntries)) {
    return -1;
  }

  root = leaf->remote_address;
  bool insert_success = leaf->insert(k, v_result);
  if (insert_success) {
    return 1;
  }

  return 2;
}

// -1 means remove failure because enterring wrong leaf node, 0 means failure
// because not enter into a leaf node, 1 means remove value succeeds, 2 means
// remove nothing
int remove(GlobalAddress &root, uint64_t dsm_base, Key k) {
  auto node_id = root.nodeID;
  auto node = root;
  NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
  while (mem_node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner<Key> *>(mem_node);
    node = inner->children[inner->lowerBound(k)];
    if (node.nodeID != node_id) {
      return 0;
    } else {
      mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
    }
  }

  BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(mem_node);
  if (!leaf->rangeValid(k)) {
    return -1;
  }

  auto flag = leaf->remove(k);
  int ret = flag ? 1 : 2;
  root = leaf->remote_address;
  return ret;
}

} // namespace cachepush