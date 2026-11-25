//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/disk/hash/disk_extendible_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief Creates a new DiskExtendibleHashTable.
 *
 * @param name
 * @param bpm buffer pool manager to be used
 * @param cmp comparator for keys
 * @param hash_fn the hash function
 * @param header_max_depth the max depth allowed for the header page
 * @param directory_max_depth the max depth allowed for the directory page
 * @param bucket_max_size the max size allowed for the bucket page array
 */
template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : index_name_(name),
      bpm_(bpm),
      cmp_(cmp),
      hash_fn_(hash_fn),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  header_page_id_ = bpm_->NewPage();
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  header_guard.AsMut<ExtendibleHTableHeaderPage>()->Init(header_max_depth_);
}

/**
 * @brief Hash a key into the 32-bit hash space used by this table.
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash(K key) const -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

/**
 * @brief The bucket page type used by this table.
 */
template <typename K, typename V, typename KC>
using BucketPage = ExtendibleHTableBucketPage<K, V, KC>;

/*****************************************************************************
 * SEARCH
 *****************************************************************************/

/**
 * Get the value associated with a given key in the hash table.
 *
 * Note(fall2023): This semester you will only need to support unique key-value pairs.
 *
 * @param key the key to look up
 * @param[out] result the value(s) associated with a given key
 * @param transaction the current transaction
 * @return the value(s) associated with the given key
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  const uint32_t hash = Hash(key);

  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto *header = header_guard.As<ExtendibleHTableHeaderPage>();
  const page_id_t directory_page_id = header->GetDirectoryPageId(header->HashToDirectoryIndex(hash));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard directory_guard = bpm_->ReadPage(directory_page_id);
  auto *directory = directory_guard.As<ExtendibleHTableDirectoryPage>();
  const page_id_t bucket_page_id = directory->GetBucketPageId(directory->HashToBucketIndex(hash));
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_guard = bpm_->ReadPage(bucket_page_id);
  auto *bucket = bucket_guard.As<BucketPage<K, V, KC>>();
  V value{};
  if (!bucket->Lookup(key, value, cmp_)) {
    return false;
  }
  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

/**
 * Inserts a key-value pair into the hash table.
 *
 * @param key the key to create
 * @param value the value to be associated with the key
 * @param transaction the current transaction
 * @return true if insert succeeded, false otherwise
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  const uint32_t hash = Hash(key);

  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  auto *header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  const uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  WritePageGuard directory_guard = bpm_->WritePage(directory_page_id);
  auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  const uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  WritePageGuard bucket_guard = bpm_->WritePage(bucket_page_id);
  auto *bucket = bucket_guard.AsMut<BucketPage<K, V, KC>>();
  if (bucket->Insert(key, value, cmp_)) {
    return true;
  }
  if (!bucket->IsFull()) {
    return false;  // the key is already present
  }

  // The bucket is full, so it has to split.
  const uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == directory->GetMaxDepth()) {
    return false;  // the table cannot grow any further
  }
  if (local_depth == directory->GetGlobalDepth()) {
    directory->IncrGlobalDepth();
  }

  const uint32_t split_bucket_idx = bucket_idx ^ (1u << local_depth);
  const page_id_t new_bucket_page_id = bpm_->NewPage();
  WritePageGuard new_bucket_guard = bpm_->WritePage(new_bucket_page_id);
  auto *new_bucket = new_bucket_guard.AsMut<BucketPage<K, V, KC>>();
  new_bucket->Init(bucket_max_size_);

  const uint32_t new_local_depth = local_depth + 1;
  MigrateEntries(bucket, new_bucket, split_bucket_idx, (1u << new_local_depth) - 1);
  UpdateDirectoryMapping(directory, split_bucket_idx, new_bucket_page_id, new_local_depth, (1u << local_depth) - 1);

  if (directory->HashToBucketIndex(hash) == split_bucket_idx) {
    return new_bucket->Insert(key, value, cmp_);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  const page_id_t directory_page_id = bpm_->NewPage();
  WritePageGuard directory_guard = bpm_->WritePage(directory_page_id);
  auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return InsertToNewBucket(directory, directory->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  const page_id_t bucket_page_id = bpm_->NewPage();
  WritePageGuard bucket_guard = bpm_->WritePage(bucket_page_id);
  auto *bucket = bucket_guard.AsMut<BucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  if (!bucket->Insert(key, value, cmp_)) {
    return false;
  }
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  const uint32_t old_local_depth = new_local_depth - 1;
  const uint32_t size = directory->Size();
  for (uint32_t i = 0; i < size; i++) {
    if (directory->GetLocalDepth(i) != old_local_depth) {
      continue;
    }
    if ((i & local_depth_mask) != (new_bucket_idx & local_depth_mask)) {
      continue;
    }
    if ((i & (1u << old_local_depth)) != 0) {
      directory->SetBucketPageId(i, new_bucket_page_id);
    }
    directory->SetLocalDepth(i, static_cast<uint8_t>(new_local_depth));
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(BucketPage<K, V, KC> *old_bucket,
                                                       BucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                                                       uint32_t local_depth_mask) {
  const uint32_t old_size = old_bucket->Size();
  for (uint32_t i = old_size; i > 0; i--) {
    const uint32_t idx = i - 1;
    const K key = old_bucket->KeyAt(idx);
    if ((Hash(key) & local_depth_mask) == new_bucket_idx) {
      const V value = old_bucket->ValueAt(idx);
      new_bucket->Insert(key, value, cmp_);
      old_bucket->RemoveAt(idx);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

/**
 * Removes a key-value pair from the hash table.
 *
 * @param key the key to delete
 * @param value the value to delete
 * @param transaction the current transaction
 * @return true if remove succeeded, false otherwise
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  const uint32_t hash = Hash(key);

  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  auto *header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  const page_id_t directory_page_id = header->GetDirectoryPageId(header->HashToDirectoryIndex(hash));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard directory_guard = bpm_->WritePage(directory_page_id);
  auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  const uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  const page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard bucket_guard = bpm_->WritePage(bucket_page_id);
  auto *bucket = bucket_guard.AsMut<BucketPage<K, V, KC>>();
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }

  // Try to merge the emptied bucket with its split image.
  const uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0 || !bucket->IsEmpty()) {
    return true;
  }

  const uint32_t sibling_idx = bucket_idx ^ (1u << (local_depth - 1));
  const page_id_t sibling_page_id = directory->GetBucketPageId(sibling_idx);
  if (sibling_page_id == INVALID_PAGE_ID || sibling_page_id == bucket_page_id ||
      directory->GetLocalDepth(sibling_idx) != local_depth) {
    return true;
  }

  {
    ReadPageGuard sibling_guard = bpm_->ReadPage(sibling_page_id);
    auto *sibling = sibling_guard.As<BucketPage<K, V, KC>>();
    if (!sibling->IsEmpty()) {
      return true;
    }
  }

  // Both halves are empty: keep the sibling page and point the whole group at it.
  const uint32_t merged_local_depth = local_depth - 1;
  const uint32_t group_mask = (1u << local_depth) - 1;
  for (uint32_t i = 0; i < directory->Size(); i++) {
    if (directory->GetLocalDepth(i) != local_depth) {
      continue;
    }
    if ((i & group_mask) != (bucket_idx & group_mask)) {
      continue;
    }
    directory->SetBucketPageId(i, sibling_page_id);
    directory->SetLocalDepth(i, static_cast<uint8_t>(merged_local_depth));
  }

  bucket_guard.Drop();
  bpm_->DeletePage(bucket_page_id);

  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  return true;
}

/*****************************************************************************
 * UTILITIES
 *****************************************************************************/

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::VerifyIntegrity() const {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto *header = header_guard.As<ExtendibleHTableHeaderPage>();

  for (uint32_t directory_idx = 0; directory_idx < header->MaxSize(); directory_idx++) {
    const page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
    if (directory_page_id == INVALID_PAGE_ID) {
      continue;
    }
    ReadPageGuard directory_guard = bpm_->ReadPage(directory_page_id);
    auto *directory = directory_guard.As<ExtendibleHTableDirectoryPage>();
    directory->VerifyIntegrity();

    for (uint32_t bucket_idx = 0; bucket_idx < directory->Size(); bucket_idx++) {
      const page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
      BUSTUB_ASSERT(bucket_page_id != INVALID_PAGE_ID, "directory points to an invalid bucket");
      ReadPageGuard bucket_guard = bpm_->ReadPage(bucket_page_id);
      auto *bucket = bucket_guard.As<BucketPage<K, V, KC>>();
      BUSTUB_ASSERT(bucket->Size() <= bucket_max_size_, "bucket holds more entries than its max size");
    }
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetHeaderPageId() const -> page_id_t {
  return header_page_id_;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::PrintHT() const {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto *header = header_guard.As<ExtendibleHTableHeaderPage>();
  header->PrintHeader();

  for (uint32_t directory_idx = 0; directory_idx < header->MaxSize(); directory_idx++) {
    const page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
    if (directory_page_id == INVALID_PAGE_ID) {
      continue;
    }
    ReadPageGuard directory_guard = bpm_->ReadPage(directory_page_id);
    auto *directory = directory_guard.As<ExtendibleHTableDirectoryPage>();
    directory->PrintDirectory();

    for (uint32_t bucket_idx = 0; bucket_idx < directory->Size(); bucket_idx++) {
      const page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
      if (bucket_page_id == INVALID_PAGE_ID) {
        continue;
      }
      ReadPageGuard bucket_guard = bpm_->ReadPage(bucket_page_id);
      bucket_guard.As<BucketPage<K, V, KC>>()->PrintBucket();
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
