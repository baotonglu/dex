#pragma once

/* This is a concurrent page table implementation;
Used to enhance the feature of leanstore => to give the ground truth of page ID
*/

#include "../../util/lock.h"
#include "../../util/utils.h"
#include "../cache/btree_node.h"
#include <algorithm>
#include <atomic>
#include <bits/hash_bytes.h>
#include <cstdint>
#include <ctime>
#include <random>

namespace cachepush {
static const int num_entry_in_page_bucket = 3;
// It should be cache-line based
// 64 Byte

const uint64_t IO_FLAG = (1ULL << 63);

struct page_frame {
  GlobalAddress page_id_;
  void *buffer_page_; // If buffer_page_ = nullptr, then this page_frame is free
};

// 64 Byte
struct page_bucket {
  Lock32 lock_;
  uint32_t count_;
  page_bucket *next_;
  page_frame page_frame_[num_entry_in_page_bucket];
};

// hash table
class page_table {
public:
  uint64_t bucket_num_;
  uint64_t entry_num_;
  page_bucket *table_;

  page_table(uint64_t entry_num) : entry_num_(entry_num) {
    bucket_num_ = ((entry_num % num_entry_in_page_bucket) == 0)
                      ? (entry_num / num_entry_in_page_bucket)
                      : (entry_num / num_entry_in_page_bucket + 1);
    assert(sizeof(page_bucket) == 64);
    posix_memalign(reinterpret_cast<void **>(&table_), 64,
                   bucket_num_ * sizeof(page_bucket));
    memset(reinterpret_cast<void *>(table_), 0,
           bucket_num_ * sizeof(page_bucket));
    std::cout << "entry_num: " << entry_num_ << std::endl;
    std::cout << "bucket_num: " << bucket_num_ << std::endl;
  }

  void traverse_and_delete(page_bucket *cur_bucket) {
    if (cur_bucket == nullptr)
      return;
    traverse_and_delete(cur_bucket->next_);
    free(cur_bucket);
  }

  void reset() {
    // Loop the page table to clear all the overflowed buckets
    for (uint64_t i = 0; i < bucket_num_; ++i) {
      auto cur_bucket = table_ + i;
      traverse_and_delete(cur_bucket->next_);
    }

    memset(reinterpret_cast<void *>(table_), 0,
           bucket_num_ * sizeof(page_bucket));
  }

  size_t hash(const void *_ptr, size_t _len,
              size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
    return std::_Hash_bytes(_ptr, _len, _seed);
  }

  page_bucket *get_bucket(GlobalAddress key) {
    auto hash_val =
        hash(reinterpret_cast<void *>(&(key.val)), sizeof(GlobalAddress));
    auto bucket_idx = hash_val % bucket_num_;
    return (table_ + bucket_idx);
  }

  bool insert(page_bucket *head_bucket, GlobalAddress key, void *&value) {
    auto cur_bucket = head_bucket;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ != nullptr)) {
          // return false;
          std::cout << "Key value = " << key.val << std::endl;
          std::cout << "K in table value = " << cur_frame[i].page_id_.val
                    << std::endl;
          value = cur_frame[i].buffer_page_;
          return false;
        }
      }
      cur_bucket = cur_bucket->next_;
    }

    cur_bucket = head_bucket;
    page_bucket *last_bucket = nullptr;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if (cur_frame[i].buffer_page_ == nullptr) {
          cur_frame[i].page_id_ = key;
          cur_frame[i].buffer_page_ = value;
          cur_bucket->count_++;
          return true;
        }
      }
      last_bucket = cur_bucket;
      cur_bucket = cur_bucket->next_;
    }

    assert(last_bucket != nullptr);
    posix_memalign(reinterpret_cast<void **>(&(last_bucket->next_)), 64,
                   sizeof(page_bucket));
    cur_bucket = last_bucket->next_;
    memset(reinterpret_cast<void *>(cur_bucket), 0, sizeof(page_bucket));
    cur_bucket->page_frame_[0].page_id_ = key;
    cur_bucket->page_frame_[0].buffer_page_ = value;
    cur_bucket->count_++;
    return true;
  }

  bool insert_io_flag(page_bucket *head_bucket, GlobalAddress key) {
    void *value = reinterpret_cast<void *>(IO_FLAG);
    return insert(head_bucket, key, value);
  }

  bool insert_with_lock(GlobalAddress key, void *&value) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = insert(head_bucket, key, value);
    head_bucket->lock_.release_lock();
    return ret;
  }

  bool insert_io_flag_with_lock(GlobalAddress key) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = insert_io_flag(head_bucket, key);
    head_bucket->lock_.release_lock();
    return ret;
  }

  bool upsert(page_bucket *head_bucket, GlobalAddress key, void *&value) {
    auto cur_bucket = head_bucket;
    // update
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ != nullptr)) {
          auto ret = cur_frame[i].buffer_page_;
          cur_frame[i].buffer_page_ = value;
          value = ret;
          return false;
        }
      }
      cur_bucket = cur_bucket->next_;
    }

    // insert
    cur_bucket = head_bucket;
    page_bucket *last_bucket = nullptr;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if (cur_frame[i].buffer_page_ == nullptr) {
          cur_frame[i].page_id_ = key;
          cur_frame[i].buffer_page_ = value;
          auto target_node = reinterpret_cast<NodeBase *>(value);
          assert(target_node->remote_address == key);
          cur_bucket->count_++;
          return true;
        }
      }
      last_bucket = cur_bucket;
      cur_bucket = cur_bucket->next_;
    }

    assert(last_bucket != nullptr);
    posix_memalign(reinterpret_cast<void **>(&(last_bucket->next_)), 64,
                   sizeof(page_bucket));
    cur_bucket = last_bucket->next_;
    memset(reinterpret_cast<void *>(cur_bucket), 0, sizeof(page_bucket));
    cur_bucket->page_frame_[0].page_id_ = key;
    cur_bucket->page_frame_[0].buffer_page_ = value;
    cur_bucket->count_++;
    return true;
  }

  bool upsert_with_lock(GlobalAddress key, void *&value) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = upsert(head_bucket, key, value);
    head_bucket->lock_.release_lock();
    return ret;
  }

  bool remove(page_bucket *head_bucket, GlobalAddress key, void *value) {
    auto cur_bucket = head_bucket;
    page_bucket *prev_bucket = nullptr;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ == value)) {
          cur_frame[i].buffer_page_ = nullptr;
          cur_bucket->count_--;
          if (cur_bucket->count_ == 0 && prev_bucket != nullptr) {
            prev_bucket->next_ = cur_bucket->next_;
            free(cur_bucket);
          }
          return true;
        }
      }
      prev_bucket = cur_bucket;
      cur_bucket = cur_bucket->next_;
    }

    return false;
  }

  bool remove_with_lock(GlobalAddress key, void *value) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = remove(head_bucket, key, value);
    head_bucket->lock_.release_lock();
    return ret;
  }

  bool remove(page_bucket *head_bucket, GlobalAddress key) {
    auto cur_bucket = head_bucket;
    page_bucket *prev_bucket = nullptr;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if (cur_frame[i].page_id_.val == key.val) {
          cur_frame[i].buffer_page_ = nullptr;
          cur_bucket->count_--;
          if (cur_bucket->count_ == 0 && prev_bucket != nullptr) {
            prev_bucket->next_ = cur_bucket->next_;
            free(cur_bucket);
          }
          return true;
        }
      }
      prev_bucket = cur_bucket;
      cur_bucket = cur_bucket->next_;
    }

    return false;
  }

  // The opt lock
  bool check_and_remove(GlobalAddress key) {
    auto head_bucket = get_bucket(key);
    auto page_ptr = get_with_lock(head_bucket, key);
    if (page_ptr == nullptr)
      return false;

    // change the way of locking
    head_bucket->lock_.get_lock();
    auto ret = remove(head_bucket, key);
    head_bucket->lock_.release_lock();
    return ret;
  }

  void *get(page_bucket *head_bucket, GlobalAddress key) {
    auto cur_bucket = head_bucket;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ != nullptr)) {
          return cur_frame[i].buffer_page_;
        }
      }
      cur_bucket = cur_bucket->next_;
    }
    return nullptr;
  }

  void *get_with_lock(GlobalAddress key) {
    auto head_bucket = get_bucket(key);
    // Below is the lock-based version
    // head_bucket->lock_.get_lock();
    // auto ret = get(head_bucket, key);
    // head_bucket->lock_.release_lock();
    // return ret;
    void *ret = nullptr;
    while (true) {
      uint32_t version = 0;
      auto flag = head_bucket->lock_.test_lock_set(version);
      if (flag)
        continue;
      ret = get(head_bucket, key);
      flag = head_bucket->lock_.test_lock_version_change(version);
      if (flag)
        continue;
      break;
    }
    return ret;
  }

  void *get_with_lock(page_bucket *head_bucket, GlobalAddress key) {
    void *ret = nullptr;
    while (true) {
      uint32_t version = 0;
      auto flag = head_bucket->lock_.test_lock_set(version);
      if (flag)
        continue;
      ret = get(head_bucket, key);
      flag = head_bucket->lock_.test_lock_version_change(version);
      if (flag)
        continue;
      break;
    }
    return ret;
  }

  bool update(page_bucket *head_bucket, GlobalAddress key, void *new_value,
              void **old_value) {
    auto cur_bucket = head_bucket;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ != nullptr)) {
          *old_value = cur_frame[i].buffer_page_;
          cur_frame[i].buffer_page_ = new_value;
          auto target_node = reinterpret_cast<NodeBase *>(new_value);
          if (target_node->remote_address != key) {
            std::cout << "New node's remote addr is not what we want!!!"
                      << std::endl;
            while (true)
              ;
          }
          assert(target_node->remote_address == key);
          return true;
        }
      }
      cur_bucket = cur_bucket->next_;
    }
    return false;
  }

  bool update_with_lock(GlobalAddress key, void *new_value, void **old_value) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = update(head_bucket, key, new_value, old_value);
    head_bucket->lock_.release_lock();
    return ret;
  }

  bool RMW(page_bucket *head_bucket, GlobalAddress key, void *new_value,
           void *old_value) {
    auto cur_bucket = head_bucket;
    while (cur_bucket) {
      auto cur_frame = cur_bucket->page_frame_;
      for (int i = 0; i < num_entry_in_page_bucket; ++i) {
        if ((cur_frame[i].page_id_.val == key.val) &&
            (cur_frame[i].buffer_page_ == old_value)) {
          cur_frame[i].buffer_page_ = new_value;
          return true;
        }
      }
      cur_bucket = cur_bucket->next_;
    }
    return false;
  }

  // only update the value when the old value matches
  bool RMW_with_lock(GlobalAddress key, void *new_value, void *old_value) {
    auto head_bucket = get_bucket(key);
    head_bucket->lock_.get_lock();
    auto ret = RMW(head_bucket, key, new_value, old_value);
    head_bucket->lock_.release_lock();
    return ret;
  }
};

} // namespace cachepush
