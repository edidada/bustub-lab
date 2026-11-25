#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 *
 * Layout (sizes in bytes):
 * | tuple_count (4) | data_used (4) | next_page_id (4) | tuple data ... | offset directory (grows backwards) |
 *
 * The tuple data area starts right after the 12-byte header, and holds the serialized tuples
 * (`Tuple::SerializeTo` writes a 4-byte length prefix followed by the tuple payload). The offset
 * directory is an array of uint32 offsets stored at the end of the page: the offset of the i-th
 * tuple lives at `BUSTUB_PAGE_SIZE - 4 * (i + 1)`.
 *
 * This class has no data members: it is reinterpret-cast onto the raw page data.
 */
class IntermediateResultPage {
 public:
  static constexpr uint32_t HEADER_SIZE = 12;

  /** Initialize the page header. */
  void Init() {
    SetTupleCount(0);
    SetDataUsed(0);
    SetNextPageId(INVALID_PAGE_ID);
  }

  /** @return the number of tuples stored in this page */
  auto GetTupleCount() const -> uint32_t { return *reinterpret_cast<const uint32_t *>(Data()); }

  /** @return the page id of the next page of the same run, or INVALID_PAGE_ID */
  auto GetNextPageId() const -> page_id_t { return *reinterpret_cast<const page_id_t *>(Data() + 8); }

  /** Set the page id of the next page of the same run. */
  void SetNextPageId(page_id_t page_id) { *reinterpret_cast<page_id_t *>(MutableData() + 8) = page_id; }

  /**
   * Append a tuple to this page.
   * @return `true` if the tuple was inserted, `false` if there is not enough space
   */
  auto InsertTuple(const Tuple &tuple) -> bool {
    const uint32_t tuple_len = tuple.GetLength();
    const uint32_t count = GetTupleCount();
    const uint32_t used = GetDataUsed();
    const uint32_t needed = HEADER_SIZE + used + sizeof(uint32_t) + tuple_len + sizeof(uint32_t) * (count + 1);
    if (needed > BUSTUB_PAGE_SIZE) {
      return false;
    }

    const uint32_t offset = HEADER_SIZE + used;
    tuple.SerializeTo(MutableData() + offset);

    const uint32_t slot_pos = BUSTUB_PAGE_SIZE - static_cast<uint32_t>(sizeof(uint32_t)) * (count + 1);
    *reinterpret_cast<uint32_t *>(MutableData() + slot_pos) = offset;

    SetTupleCount(count + 1);
    SetDataUsed(used + static_cast<uint32_t>(sizeof(uint32_t)) + tuple_len);
    return true;
  }

  /** @return the i-th tuple stored in this page */
  auto GetTuple(uint32_t index) const -> Tuple {
    const uint32_t slot_pos = BUSTUB_PAGE_SIZE - static_cast<uint32_t>(sizeof(uint32_t)) * (index + 1);
    const uint32_t offset = *reinterpret_cast<const uint32_t *>(Data() + slot_pos);
    Tuple tuple;
    tuple.DeserializeFrom(Data() + offset);
    return tuple;
  }

 private:
  auto Data() const -> const char * { return reinterpret_cast<const char *>(this); }

  auto MutableData() -> char * { return reinterpret_cast<char *>(this); }

  void SetTupleCount(uint32_t count) { *reinterpret_cast<uint32_t *>(MutableData()) = count; }

  auto GetDataUsed() const -> uint32_t { return *reinterpret_cast<const uint32_t *>(Data() + 4); }

  void SetDataUsed(uint32_t used) { *reinterpret_cast<uint32_t *>(MutableData() + 4) = used; }
};

}  // namespace bustub
