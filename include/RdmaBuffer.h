#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"
#include "smart/smartCommon.h"
#include <iostream>

// abstract rdma registered buffer
// #define DEX_BUFFER 1
class RdmaBuffer {

private:
  static const int kPageBufferCnt = 8;    // async, buffer safty
  static const int kSiblingBufferCnt = 8; // async, buffer safty
  static const int kCasBufferCnt = 256;   // async, buffer safty

  // For SMART
  static const int kSmartPageBufferCnt =
      256; // big enough to hold batch internal node write in
           // out_of_place_write_node
  static const int kLeafBufferCnt = 32;
  static const int kHeaderBufferCnt = 32;
  static const int kEntryBufferCnt = 32;

  char *buffer;

  // uint64_t *cas_buffer;    // 8B unit
  uint64_t *unlock_buffer; // total 8B
  uint64_t *zero_64bit;
  // char *page_buffer;    // 1KB unit for BTree
  // char *sibling_buffer; // 1KB unit for BTree
  // char *entry_buffer;

  uint64_t *cas_buffer;
  char *page_buffer;
  char *sibling_buffer;
  char *smart_page_buffer;
  char *leaf_buffer;
  uint64_t *header_buffer;
  uint64_t *entry_buffer;
  char *range_buffer;
  char *zero_byte;

  int page_buffer_cur;
  int sibling_buffer_cur;
  int cas_buffer_cur;
  int smart_page_buffer_cur;
  int leaf_buffer_cur;
  int header_buffer_cur;
  int entry_buffer_cur;

  int kPageSize;

public:
  // RdmaBuffer(char *buffer) {
  //   std::cout << "I am setting buffer here" << std::endl;
  //   set_buffer(buffer);

  //   page_buffer_cur = 0;
  //   sibling_buffer_cur = 0;
  //   cas_buffer_cur = 0;
  // }

  RdmaBuffer() = default;

  void set_dex_buffer(char *buffer) {
    kPageSize = std::max(kLeafPageSize, kInternalPageSize);
    this->buffer = buffer;
    page_buffer = (char *)buffer;
    sibling_buffer = (char *)page_buffer + kPageSize * kPageBufferCnt;
    cas_buffer = (uint64_t *)reinterpret_cast<uint64_t *>(
        (char *)sibling_buffer + kPageSize * kSiblingBufferCnt);
    // cas_buffer = (uint64_t *)buffer;
    unlock_buffer =
        (uint64_t *)((char *)cas_buffer + sizeof(uint64_t) * kCasBufferCnt);
    zero_64bit = (uint64_t *)((char *)unlock_buffer + sizeof(uint64_t));
    entry_buffer =
        reinterpret_cast<uint64_t *>((char *)zero_64bit + sizeof(int64_t));
    *zero_64bit = 0;
    assert((char *)zero_64bit + 8 - buffer < define::kPerCoroRdmaBuf);
  }

  void set_smart_buffer(char *buffer) {
    this->buffer = buffer;
    cas_buffer = (uint64_t *)buffer;
    smart_page_buffer =
        (char *)((char *)cas_buffer + sizeof(uint64_t) * kCasBufferCnt);
    leaf_buffer = (char *)((char *)smart_page_buffer +
                           define::allocationPageSize * kSmartPageBufferCnt);
    header_buffer = (uint64_t *)((char *)leaf_buffer +
                                 define::allocAlignLeafSize * kLeafBufferCnt);
    entry_buffer = (uint64_t *)((char *)header_buffer +
                                sizeof(uint64_t) * kHeaderBufferCnt);
    zero_byte =
        (char *)((char *)entry_buffer + sizeof(uint64_t) * kEntryBufferCnt);
    range_buffer = (char *)((char *)zero_byte + sizeof(char));
    *zero_byte = '\0';

    assert(range_buffer - buffer < define::kSmartPerCoroRdmaBuf);
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt;
    return cas_buffer + cas_buffer_cur;
  }

  // uint64_t *get_unlock_buffer() const { return unlock_buffer; }

  // uint64_t *get_zero_64bit() const { return zero_64bit; }

  char *get_page_buffer() {
    page_buffer_cur = (page_buffer_cur + 1) % kPageBufferCnt;
    return page_buffer + (page_buffer_cur * kPageSize);
  }

  // char *get_range_buffer() { return page_buffer; }

  char *get_sibling_buffer() {
    sibling_buffer_cur = (sibling_buffer_cur + 1) % kSiblingBufferCnt;
    return sibling_buffer + (sibling_buffer_cur * kPageSize);
  }

  // uint64_t *get_entry_buffer() const { return entry_buffer; }

  char *get_smart_page_buffer() {
    smart_page_buffer_cur = (smart_page_buffer_cur + 1) % kSmartPageBufferCnt;
    return smart_page_buffer +
           smart_page_buffer_cur * define::allocationPageSize;
  }

  char *get_leaf_buffer() {
    leaf_buffer_cur = (leaf_buffer_cur + 1) % kLeafBufferCnt;
    return leaf_buffer + leaf_buffer_cur * define::allocAlignLeafSize;
  }

  uint64_t *get_header_buffer() {
    header_buffer_cur = (header_buffer_cur + 1) % kHeaderBufferCnt;
    return header_buffer + header_buffer_cur;
  }

  uint64_t *get_entry_buffer() {
    entry_buffer_cur = (entry_buffer_cur + 1) % kEntryBufferCnt;
    return entry_buffer + entry_buffer_cur;
  }

  char *get_range_buffer() { return range_buffer; }

  char *get_zero_byte() { return zero_byte; }
};

#endif // _RDMA_BUFFER_H_