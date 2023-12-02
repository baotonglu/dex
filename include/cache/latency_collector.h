#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <queue>
#include <random>
#include <thread>
#include <utility>

namespace cachepush {

void clear_queue(std::queue<std::chrono::nanoseconds> &q) {
  std::queue<std::chrono::nanoseconds> empty;
  std::swap(q, empty);
}

class LatencyCollector {
public:
  // Those latency should also inlcude the node search latency?
  // TODO: the queue can be optimized using a ring queue
  LatencyCollector()
      : sum_pushdown_ns_(0), sum_caching_ns_(0), total_num_(50),
        generator(clock() + pthread_self()), distribution(0, 999) {}

  void add_pushdown_latency(std::chrono::nanoseconds sample_latency) {
    if (pushdown_queue_.size() == total_num_) {
      // Compute the standard deviation
      auto pop_one = pushdown_queue_.front();
      pushdown_queue_.pop();
      sum_pushdown_ns_ = sum_pushdown_ns_ - pop_one;
    }
    pushdown_queue_.push(sample_latency);
    sum_pushdown_ns_ = sum_pushdown_ns_ + sample_latency;
  }

  void add_caching_latency(std::chrono::nanoseconds sample_latency) {
    if (caching_queue_.size() == total_num_) {
      auto pop_one = caching_queue_.front();
      caching_queue_.pop();
      sum_caching_ns_ = sum_caching_ns_ - pop_one;
    }
    caching_queue_.push(sample_latency);
    sum_caching_ns_ = sum_caching_ns_ + sample_latency;
  }

  std::chrono::nanoseconds avg_pushdown_latency() {
    if (pushdown_queue_.empty())
      return std::chrono::nanoseconds(0);
    return std::chrono::nanoseconds(sum_pushdown_ns_.count() /
                                    pushdown_queue_.size());
  }

  std::chrono::nanoseconds avg_caching_latency() {
    if (caching_queue_.empty())
      return std::chrono::nanoseconds(0);
    return std::chrono::nanoseconds(sum_caching_ns_.count() /
                                    caching_queue_.size());
  }

  void print_latency_queue() {
    std::cout << "Caching queue: ";
    std::queue<std::chrono::nanoseconds> tmp_caching = caching_queue_;
    while (!tmp_caching.empty()) {
      std::cout << tmp_caching.front().count() << " ";
      tmp_caching.pop();
    }
    std::cout << std::endl;

    std::cout << "Pushdown queue: ";
    std::queue<std::chrono::nanoseconds> tmp_push = pushdown_queue_;
    while (!tmp_push.empty()) {
      std::cout << tmp_push.front().count() << " ";
      tmp_push.pop();
    }
    std::cout << std::endl;
  }

  void clear() {
    clear_queue(caching_queue_);
    clear_queue(pushdown_queue_);
    caching_times = 0;
    pushdown_times = 0;
    sum_pushdown_ns_ = std::chrono::nanoseconds::zero();
    sum_caching_ns_ = std::chrono::nanoseconds::zero();
  }

  void set_total_num(int64_t total_num) { total_num_ = total_num; }

  // ret = 0, caching
  // ret = 1, pushdown
  int caching_or_push(int missing_num, bool &sample) {
    if (caching_queue_.size() < total_num_) {
      sample = true;
      caching_times++;
      return 0;
    }

    if (pushdown_queue_.size() < total_num_) {
      sample = true;
      pushdown_times++;
      return 1;
    }

    // determine whether to sample the lantency (1% sampling probablity)
    auto random_num = distribution(generator);
    if (random_num < 10)
      sample = true;

    auto pushdown_latency = avg_pushdown_latency();
    auto caching_latency = avg_caching_latency();
    auto ret = 0;

    // 40ns is the latency to binary search a 1KB B+-Tree node in cache
    if (pushdown_latency.count() <
        (missing_num * (caching_latency.count() + 40) * 1.2)) {
      ret = 1;
    }

    random_num = distribution(generator);
    if (random_num < 10) { // 1% probability of random exploration
      ret = ((ret == 0) ? 1 : 0);
      sample = true;
    }

    if (ret) {
      pushdown_times++;
    } else {
      caching_times++;
    }

    return ret; // select caching
  }

  double pushdown_rate() {
    if (pushdown_times == 0)
      return 0;
    return pushdown_times / static_cast<double>(caching_times + pushdown_times);
  }

  void show_statistic() {
    std::cout << "Pushdown rate = " << pushdown_rate() << std::endl;
    std::cout << "Pushdown latency(ns) = " << avg_pushdown_latency().count()
              << std::endl;
    std::cout << "Caching latenc(ns) = " << avg_caching_latency().count()
              << std::endl;
  }

  std::queue<std::chrono::nanoseconds> pushdown_queue_;
  std::queue<std::chrono::nanoseconds> caching_queue_;
  // uint64_t num_;
  int64_t total_num_;
  uint64_t pushdown_times;
  uint64_t caching_times;
  std::chrono::nanoseconds sum_pushdown_ns_;
  std::chrono::nanoseconds sum_caching_ns_;
  std::mt19937 generator;
  std::uniform_int_distribution<uint64_t> distribution;
};

} // namespace cachepush