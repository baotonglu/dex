#pragma once

#include "../Common.h"
#include "../tree_api.h"
#include "smart.h"
#include <iostream>
#include <limits>
#include <map>
#include <thread>

template <class T, class P> class smart_wrapper : public tree_api<T, P> {
public:
  struct smart_partition_info {
    uint64_t *array;
    uint64_t num;
    int id;
  };

  smart_wrapper(DSM *dsm, uint16_t tree_id, int cache_size) {
    my_tree = new smart::Tree(dsm, tree_id, cache_size);
    my_dsm = dsm;
  }

  bool insert(T key, P value) {
    smart::Key smart_k = smart::int2key(key);
    my_tree->insert(smart_k, value, nullptr, 0, false, false);
    return true;
  }

  bool lookup(T key, P &value) {
    smart::Key smart_k = smart::int2key(key);
    return my_tree->search(smart_k, value, nullptr, 0);
  }

  bool update(T key, P value) {
    smart::Key smart_k = smart::int2key(key);
    my_tree->insert(smart_k, value, nullptr, 0, true, false);
    return true;
  }

  bool remove(T key) {
    // my_tree->del(key);
    return true;
  }

  virtual int range_scan(T key, uint32_t num, std::pair<T, P> *&result) {
    T end_key =
        ((key + num) < key) ? std::numeric_limits<T>::max() : (key + num);
    std::map<smart::Key, smart::Value> ret;
    smart::Key s_key = smart::int2key(key);
    smart::Key e_key = smart::int2key(end_key);
    my_tree->range_query(s_key, e_key, ret);
    int count = 0;
    for (auto it = ret.begin(); it != ret.end(); it++) {
      result[count++] = std::make_pair(smart::key2int(it->first),
                                       static_cast<Value>(it->second));
    }
    return count;
  }

  void bulk_load(T *bulk_array, uint64_t bulk_load_num) {
    // uint64_t cluster_num = my_dsm->getClusterSize();
    uint32_t node_id = my_dsm->getMyNodeID();
    uint32_t compute_num = my_dsm->getComputeNum();
    if (node_id >= compute_num) {
      return;
    }
    // std::cout << "Smart real leaf size = " << sizeof(smart::Leaf) <<
    // std::endl;

    smart_partition_info *all_partition =
        new smart_partition_info[bulk_threads];
    uint64_t each_partition = bulk_load_num / (bulk_threads * compute_num);

    for (uint64_t i = 0; i < bulk_threads; ++i) {
      all_partition[i].id = i + node_id * bulk_threads;
      all_partition[i].array =
          bulk_array + (all_partition[i].id * each_partition);
      all_partition[i].num = each_partition;
    }

    if (node_id == (compute_num - 1)) {
      all_partition[bulk_threads - 1].num =
          bulk_load_num - (each_partition * (bulk_threads * compute_num - 1));
    }

    auto bulk_thread = [&](void *bulk_info) {
      auto my_parition = reinterpret_cast<smart_partition_info *>(bulk_info);
      bindCore((my_parition->id % bulk_threads) * 2);
      my_dsm->registerThread();
      auto num = my_parition->num;
      auto array = my_parition->array;

      for (uint64_t i = 0; i < num; ++i) {
        smart::Key smart_k = smart::int2key(array[i]);
        // std::cout << i << " start insert key-------------- " << array[i]
        //           << std::endl;
        my_tree->insert(smart_k, array[i] + 1, nullptr, 0, false, true);
        // std::cout << i << " finish insert key------------- " << array[i]
        //           << std::endl;
        // std::cout << std::endl;
        if ((i + 1) % 1000000 == 0) {
          std::cout << "Thread " << my_parition->id << " finishes insert " << i
                    << " keys" << std::endl;
        }
      }
    };

    for (uint64_t i = 0; i < bulk_threads; i++) {
      th[i] =
          std::thread(bulk_thread, reinterpret_cast<void *>(all_partition + i));
    }

    for (uint64_t i = 0; i < bulk_threads; i++) {
      th[i].join();
    }
  }

  void get_statistic() {
    // Report the cache miss ratio
    uint64_t total_miss = 0;
    uint64_t total_hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      total_miss += my_tree->cache_miss[i][0];
      total_hit += my_tree->cache_hit[i][0];
    }
    std::cout << "cache hit ratio: "
              << static_cast<double>(total_hit) /
                     static_cast<double>(total_miss + total_hit)
              << std::endl;
  }

  void clear_statistic() { my_tree->clear_debug_info(); }

  smart::Tree *my_tree;
  DSM *my_dsm;
  uint64_t bulk_threads = 8;
  std::thread th[8];
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};