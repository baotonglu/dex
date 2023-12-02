#pragma once

#include "Common.h"
#include "Tree.h"
#include "tree_api.h"
#include <limits>
#include <thread>

template <class T, class P> class sherman_wrapper : public tree_api<T, P> {
public:
  struct partition_info {
    uint64_t *array;
    uint64_t num;
    int id;
  };

  sherman_wrapper(DSM *dsm, uint16_t tree_id, int cache_size) {
    my_tree = new sherman::Tree(dsm, tree_id, cache_size);
    my_dsm = dsm;
  }

  bool insert(T key, P value) {
    my_tree->insert(key, value);
    return true;
  }

  bool lookup(T key, P &value) { return my_tree->search(key, value); }

  bool update(T key, P value) {
    my_tree->insert(key, value);
    return true;
  }

  bool remove(T key) {
    my_tree->del(key);
    return true;
  }

  int range_scan(T key, uint32_t num, std::pair<T, P> *&result) {
    T end_key =
        ((key + num) < key) ? std::numeric_limits<T>::max() : (key + num);
    return my_tree->range_query(key, end_key, result);
  }

  void clear_statistic() { my_tree->clear_statistics(); }

  void bulk_load(T *bulk_array, uint64_t bulk_load_num) {
    // uint64_t cluster_num = my_dsm->getClusterSize();
    // uint64_t node_id = my_dsm->getMyNodeID();
    // if (node_id != 0)
    //   return;

    uint32_t node_id = my_dsm->getMyNodeID();
    uint32_t compute_num = my_dsm->getComputeNum();
    if (node_id >= compute_num) {
      return;
    }

    // partition_info *all_partition = new partition_info[bulk_threads];
    // uint64_t each_partition = bulk_load_num / (bulk_threads);
    partition_info *all_partition = new partition_info[bulk_threads];
    uint64_t each_partition = bulk_load_num / (bulk_threads * compute_num);

    // for (uint64_t i = 0; i < bulk_threads; ++i) {
    //   all_partition[i].id = i;
    //   all_partition[i].array =
    //       bulk_array + (all_partition[i].id * each_partition);
    //   all_partition[i].num = each_partition;
    // }
    // all_partition[bulk_threads - 1].num =
    //     bulk_load_num - (each_partition * (bulk_threads - 1));

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
      auto my_parition = reinterpret_cast<partition_info *>(bulk_info);
      // bindCore(my_parition->id);
      bindCore((my_parition->id % bulk_threads) * 2);
      my_dsm->registerThread();
      auto num = my_parition->num;
      auto array = my_parition->array;

      for (uint64_t i = 0; i < num; ++i) {
        my_tree->insert(array[i], array[i] + 1);
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

  sherman::Tree *my_tree;
  DSM *my_dsm;
  uint64_t bulk_threads = 8;
  std::thread th[8];
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};