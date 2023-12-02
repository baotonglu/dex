#pragma once
#include <random>

class uniform_key_generator_t {
public:
  uniform_key_generator_t(size_t N) : dist_(1, N) {
    generator_.seed((uint32_t)0xc70f6907);
  }

  uint64_t next_id() { return dist_(generator_); }

private:
  std::uniform_int_distribution<uint64_t> dist_;
  std::default_random_engine generator_;
};