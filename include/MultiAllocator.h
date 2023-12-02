#if !defined(_MULTI_ALLOC_H_)
#define _MULTI_ALLOC_H_

#include "Common.h"
#include "GlobalAddress.h"

#include <vector>

// for fine-grained shared memory alloc
// not thread safe => So we use thread-local for each thread
// now it is a simple log-structure alloctor
// TODO: slab-based alloctor
// Predefine: there are at most 128 memory nodes in this clusder
class MultiAllocator {

public:
  MultiAllocator() {
    for (int i = 0; i < 128; ++i) {
      cur.push_back(GlobalAddress::Null());
      head.push_back(GlobalAddress::Null());
      head_offset.push_back(0);
      log_heads.push_back({});
    }
  }

  // Each chunk is 32MB
  GlobalAddress malloc(size_t size, bool &need_chunck, int node_id) {
    GlobalAddress res = cur[node_id];
#ifdef PAGE_OFFSET
    if (log_heads[node_id].empty()) {
      need_chunck = true;
    } else if (cur[node_id].offset + size + 8 >
               head[node_id].offset + define::kChunkSize) {
      head_offset[node_id] =
          size - ((cur[node_id].offset + size + 8) -
                  (head[node_id].offset + define::kChunkSize));
      need_chunck = true;
    } else {
      need_chunck = false;
      cur[node_id].offset += (size + 8);
    }
#else
    if (log_heads[node_id].empty() ||
        (cur[node_id].offset + size + 8 >
         head[node_id].offset + define::kChunkSize)) {
      need_chunck = true;
    } else {
      need_chunck = false;
      cur[node_id].offset += size;
    }
#endif
    // assert(res.addr + size <= 40 * define::GB);
    return res;
  }

  void set_chunck(GlobalAddress &addr, int node_id) {
    log_heads[node_id].push_back(addr);
    addr.offset += head_offset[node_id];
    head[node_id] = cur[node_id] = addr;
  }

  void free(const GlobalAddress &addr) {
    // TODO
  }

private:
  std::vector<GlobalAddress> cur;
  std::vector<GlobalAddress> head;
  std::vector<std::vector<GlobalAddress>> log_heads;
  std::vector<uint64_t> head_offset;
};

#endif // _LOCAL_ALLOC_H_
