#include "latency_collector.h"

int main() {
  cachepush::LatencyCollector my_collector(5);

  bool sample = false;
  auto first_dec = my_collector.caching_or_push(1, sample);
  std::cout << "First decision = " << first_dec << std::endl;

  for (int i = 0; i < 15; ++i) {
    sample = false;
    auto dec = my_collector.caching_or_push(2, sample);
    std::cout << i << " decision = " << dec << std::endl;
    if (dec == 0) {
      // caching
      my_collector.add_caching_latency(std::chrono::nanoseconds(i));
    } else {
      // pushdown
      my_collector.add_pushdown_latency(std::chrono::nanoseconds(i));
    }
    std::cout << "Start compute the average" << std::endl;
    auto avg_pushdown_latency = my_collector.avg_pushdown_latency();
    auto avg_caching_latency = my_collector.avg_caching_latency();
    std::cout << "Avg caching latency = " << avg_caching_latency.count()
              << std::endl;
    std::cout << "Avg pushdown latency = " << avg_pushdown_latency.count()
              << std::endl;
    my_collector.print_latency_queue();
    std::cout << "----------------------------------------" << std::endl;
  }

  auto dec = my_collector.caching_or_push(2, sample);
  std::cout << "Final dec = " << dec << std::endl;
  auto avg_pushdown_latency = my_collector.avg_pushdown_latency();
  auto avg_caching_latency = my_collector.avg_caching_latency();
  std::cout << "Avg caching latency = " << avg_caching_latency.count()
            << std::endl;
  std::cout << "Avg pushdown latency = " << avg_pushdown_latency.count()
            << std::endl;
  return 0;
}