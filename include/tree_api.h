#pragma once

#include <cstdint>
#include <map>
#include <utility>

template <class T, class P> class tree_api;

extern "C" void *create_tree();

// used to define the interface of all benchmarking trees
template <class T, class P> class tree_api {
public:
  virtual bool insert(T key, P value) = 0;
  virtual bool lookup(T key, P &value) = 0;
  virtual bool update(T key, P value) = 0;
  virtual bool remove(T key) = 0;
  // Range query
  // Return #keys really scanned
  virtual int range_scan(T key, uint32_t num, std::pair<T, P> *&result) = 0;
  virtual void bulk_load(T *key, uint64_t num) = 0;
  virtual void clear_statistic() = 0;

  virtual void set_shared(std::vector<T> &bound) {}
  virtual void flush_all() {}
  virtual void get_basic() {}
  virtual void get_newest_root() {}
  virtual void set_bound(T left, T right) {}

  virtual void validate() {}

  virtual void reset_buffer_pool(bool flush_dirty) {}

  virtual void set_rpc_ratio(double ratio) {}
  virtual void set_admission_ratio(double ratio) {}
  virtual double get_rpc_ratio() { return 0; }

  virtual void get_statistic() {}

  // Do most initialization work here
  tree_api *create_tree() { return nullptr; }
};