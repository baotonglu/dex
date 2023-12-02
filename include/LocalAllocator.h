#if !defined(_LOCAL_ALLOC_H_)
#define _LOCAL_ALLOC_H_

#include "Common.h"
#include "GlobalAddress.h"

#include <vector>

// for fine-grained shared memory alloc
// not thread safe
// now it is a simple log-structure alloctor
// TODO: slab-based alloctor
class LocalAllocator {

public:
  LocalAllocator() {
    head = GlobalAddress::Null();
    cur = GlobalAddress::Null();
    head_offset = 0;
  }
  // Each chunk is 32MB
  GlobalAddress malloc(size_t size, bool &need_chunck, bool align = false) {
    if (align) {
    }

    GlobalAddress res = cur;

#ifdef PAGE_OFFSET
    if (log_heads.empty()) {
      need_chunck = true;
    } else if (cur.offset + size + 8 > head.offset + define::kChunkSize) {
      head_offset =
          size - ((cur.offset + size + 8) - (head.offset + define::kChunkSize));
      need_chunck = true;
    } else {
      need_chunck = false;
      cur.offset += (size + 8);
    }
#else
    if (log_heads.empty() ||
        (cur.offset + size + 8 > head.offset + define::kChunkSize)) {
      need_chunck = true;
    } else {
      need_chunck = false;
      cur.offset += size;
    }
#endif

    // assert(res.addr + size <= 40 * define::GB);
    return res;
  }

  void set_chunck(GlobalAddress &addr) {
    log_heads.push_back(addr);
    addr.offset += head_offset;
    head = cur = addr;
  }

  void free(const GlobalAddress &addr) {
    // TODO
  }

  void free(const GlobalAddress &addr, size_t size) {}

private:
  GlobalAddress head;
  GlobalAddress cur;
  std::vector<GlobalAddress> log_heads;
  uint64_t head_offset;
};

#endif // _LOCAL_ALLOC_H_
