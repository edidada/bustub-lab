//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include <iostream>

#include "common/exception.h"

namespace bustub {

/**
 * After creating a new header page from buffer pool, must call initialize
 * method to set default values
 * @param max_depth Max depth in the header page
 */
void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  for (uint32_t i = 0; i < HTABLE_HEADER_ARRAY_SIZE; i++) {
    directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

/**
 * Get the directory index that the key is hashed to
 *
 * @param hash the hash of the key
 * @return directory index the key is hashed to
 */
auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (max_depth_ == 0) {
    return 0;
  }
  return hash >> (32 - max_depth_);
}

/**
 * Get the directory page id at an index
 *
 * @param directory_idx index in the directory page id array
 * @return directory page_id at index
 */
auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> page_id_t {
  BUSTUB_ASSERT(directory_idx < MaxSize(), "directory index out of range");
  return directory_page_ids_[directory_idx];
}

/**
 * @brief Set the directory page id at an index
 *
 * @param directory_idx index in the directory page id array
 * @param directory_page_id page id of the directory
 */
void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  BUSTUB_ASSERT(directory_idx < MaxSize(), "directory index out of range");
  directory_page_ids_[directory_idx] = directory_page_id;
}

/**
 * @brief Get the maximum number of directory page ids the header page could handle
 */
auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1u << max_depth_; }

void ExtendibleHTableHeaderPage::PrintHeader() const {
  std::cout << "======== HEADER (max_depth_: " << max_depth_ << ") ========\n";
  for (uint32_t i = 0; i < MaxSize(); i++) {
    std::cout << "  directory_idx: " << i << " -> page_id: " << directory_page_ids_[i] << "\n";
  }
  std::cout << "=============== END HEADER ===============\n";
}

}  // namespace bustub
