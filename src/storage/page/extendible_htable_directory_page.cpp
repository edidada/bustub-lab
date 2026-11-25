//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <iostream>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * After creating a new directory page from buffer pool, must call initialize
 * method to set default values
 * @param max_depth Max depth in the directory page
 */
void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint32_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
    local_depths_[i] = 0;
  }
}

/**
 * Get the bucket index that the key is hashed to
 *
 * @param hash the hash of the key
 * @return bucket index current key is hashed to
 */
auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

/**
 * Lookup a bucket page using a directory index
 *
 * @param bucket_idx the index in the directory to lookup
 * @return bucket page_id corresponding to bucket_idx
 */
auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  return bucket_page_ids_[bucket_idx];
}

/**
 * Updates the directory index using a bucket index and page_id
 *
 * @param bucket_idx directory index at which to insert page_id
 * @param bucket_page_id page_id to insert
 */
void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

/**
 * Gets the split image of an index
 *
 * @param bucket_idx the directory index for which to find the split image
 * @return the directory index of the split image
 **/
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1u << local_depths_[bucket_idx]);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1u << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1u << local_depths_[bucket_idx]) - 1;
}

/**
 * Get the global depth of the hash table directory
 *
 * @return the global depth of the directory
 */
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

/**
 * Increment the global depth of the directory
 */
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  BUSTUB_ASSERT(global_depth_ < max_depth_, "cannot grow the directory beyond max depth");
  const uint32_t size = Size();
  for (uint32_t i = 0; i < size; i++) {
    bucket_page_ids_[i + size] = bucket_page_ids_[i];
    local_depths_[i + size] = local_depths_[i];
  }
  global_depth_++;
}

/**
 * Decrement the global depth of the directory
 */
void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  BUSTUB_ASSERT(global_depth_ > 0, "cannot shrink a directory with global depth 0");
  global_depth_--;
}

/**
 * @return true if the directory can be shrunk
 */
auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

/**
 * @return the current directory size
 */
auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1u << global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1u << max_depth_; }

/**
 * Gets the local depth of the bucket at bucket_idx
 *
 * @param bucket_idx the bucket index to lookup
 * @return the local depth of the bucket at bucket_idx
 */
auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  return local_depths_[bucket_idx];
}

/**
 * Set the local depth of the bucket at bucket_idx to local_depth
 *
 * @param bucket_idx bucket index to update
 * @param local_depth new local depth
 */
void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  BUSTUB_ASSERT(local_depth <= max_depth_, "local depth cannot exceed max depth");
  local_depths_[bucket_idx] = local_depth;
}

/**
 * Increment the local depth of the bucket at bucket_idx
 * @param bucket_idx bucket index to increment
 */
void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  BUSTUB_ASSERT(local_depths_[bucket_idx] < global_depth_, "local depth cannot exceed global depth");
  local_depths_[bucket_idx]++;
}

/**
 * Decrement the local depth of the bucket at bucket_idx
 * @param bucket_idx bucket index to decrement
 */
void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  BUSTUB_ASSERT(local_depths_[bucket_idx] > 0, "local depth cannot go below zero");
  local_depths_[bucket_idx]--;
}

void ExtendibleHTableDirectoryPage::VerifyIntegrity() const {
  BUSTUB_ASSERT(global_depth_ <= max_depth_, "global depth exceeds max depth");
  const uint32_t size = Size();
  std::unordered_map<page_id_t, std::pair<uint32_t, uint32_t>> seen;  // page id -> (count, local depth)
  for (uint32_t i = 0; i < size; i++) {
    const uint32_t local_depth = local_depths_[i];
    BUSTUB_ASSERT(local_depth <= global_depth_, "local depth exceeds global depth");
    const page_id_t page_id = bucket_page_ids_[i];
    auto it = seen.find(page_id);
    if (it == seen.end()) {
      seen[page_id] = {1, local_depth};
    } else {
      BUSTUB_ASSERT(it->second.second == local_depth, "same bucket with different local depths");
      it->second.first++;
    }
  }
  for (const auto &entry : seen) {
    BUSTUB_ASSERT(entry.second.first == (1u << (global_depth_ - entry.second.second)),
                  "bucket does not have exactly 2^(global_depth - local_depth) pointers");
  }
}

void ExtendibleHTableDirectoryPage::PrintDirectory() const {
  std::cout << "======== DIRECTORY (global_depth_: " << global_depth_ << ") ========\n";
  std::cout << "| bucket_idx | page_id | local_depth |\n";
  for (uint32_t i = 0; i < Size(); i++) {
    std::cout << "|    " << i << "    |    " << bucket_page_ids_[i] << "    |    " << local_depths_[i] << "    |\n";
  }
  std::cout << "=============== END DIRECTORY ===============\n";
}

}  // namespace bustub
