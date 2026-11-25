//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_bucket_page.h"

#include <algorithm>
#include <iostream>
#include <optional>
#include <utility>

#include "common/exception.h"

namespace bustub {

/**
 * After creating a new bucket page from buffer pool, must call initialize
 * method to set default values
 * @param max_size Max size of the bucket array
 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  size_ = 0;
  max_size_ = std::min(max_size, static_cast<uint32_t>(HTableBucketArraySize(sizeof(MappingType))));
}

/**
 * Lookup a key
 *
 * @param key key to lookup
 * @param[out] value value to set
 * @param cmp the comparator
 * @return true if the key and value are present, false if not found.
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

/**
 * Attempts to insert a key and value in the bucket.
 *
 * @param key key to insert
 * @param value value to insert
 * @param cmp the comparator to use
 * @return true if inserted, false if bucket is full or the same key is already present
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      return false;
    }
  }
  if (IsFull()) {
    return false;
  }
  array_[size_] = std::make_pair(key, value);
  size_++;
  return true;
}

/**
 * Removes a key and value.
 *
 * @return true if removed, false if not found
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < size_, "index out of range");
  for (uint32_t i = bucket_idx; i + 1 < size_; i++) {
    array_[i] = array_[i + 1];
  }
  size_--;
}

/**
 * @brief Gets the key at an index in the bucket.
 *
 * @param bucket_idx the index in the bucket to get the key at
 * @return key at index bucket_idx of the bucket
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  BUSTUB_ASSERT(bucket_idx < size_, "index out of range");
  return array_[bucket_idx].first;
}

/**
 * Gets the value at an index in the bucket.
 *
 * @param bucket_idx the index in the bucket to get the value at
 * @return value at index bucket_idx of the bucket
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  BUSTUB_ASSERT(bucket_idx < size_, "index out of range");
  return array_[bucket_idx].second;
}

/**
 * Gets the entry at an index in the bucket.
 *
 * @param bucket_idx the index in the bucket to get the entry at
 * @return entry at index bucket_idx of the bucket
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  BUSTUB_ASSERT(bucket_idx < size_, "index out of range");
  return array_[bucket_idx];
}

/**
 * @return number of entries in the bucket
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

/**
 * @return whether the bucket is full
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

/**
 * @return whether the bucket is empty
 */
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

/**
 * Prints the bucket's occupancy information
 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::PrintBucket() const {
  std::cout << "Bucket size: " << size_ << " / " << max_size_ << "\n";
  for (uint32_t i = 0; i < size_; i++) {
    std::cout << "  " << i << ": " << array_[i].first << " -> " << array_[i].second << "\n";
  }
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
