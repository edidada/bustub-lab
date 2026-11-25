//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  // Leave one spare slot so that an insertion into a full page can still shift entries right.
  SetMaxSize(std::min(max_size, static_cast<int>(LEAF_PAGE_SLOT_CNT) - 1));
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> result;
  for (size_t i = 0; i < num_tombstones_; i++) {
    result.push_back(key_array_[tombstones_[i]]);
  }
  return result;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return rid_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyRefAt(int index) const -> const KeyType & { return key_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueRefAt(int index) const -> const ValueType & { return rid_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  const int n = GetSize();
  for (int i = n; i > index; i--) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  // Entries after the insertion point shift right by one, so do their tombstones.
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (static_cast<int>(tombstones_[i]) >= index) {
      tombstones_[i]++;
    }
  }
  SetSize(n + 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  const int n = GetSize();
  for (int i = index; i + 1 < n; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (static_cast<int>(tombstones_[i]) > index) {
      tombstones_[i]--;
    }
  }
  SetSize(n - 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstone(int index) const -> bool {
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (static_cast<int>(tombstones_[i]) == index) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AddTombstone(int index) {
  if (num_tombstones_ < LEAF_PAGE_TOMB_CNT) {
    tombstones_[num_tombstones_++] = static_cast<size_t>(index);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CanAddTombstone() const -> bool {
  return num_tombstones_ < static_cast<size_t>(LEAF_PAGE_TOMB_CNT);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ClearTombstone(int index) {
  size_t out = 0;
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (static_cast<int>(tombstones_[i]) != index) {
      tombstones_[out++] = tombstones_[i];
    }
  }
  num_tombstones_ = out;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { rid_array_[index] = value; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PurgeTombstones() {
  if (num_tombstones_ == 0) {
    return;
  }
  std::vector<int> indexes;
  indexes.reserve(num_tombstones_);
  for (size_t i = 0; i < num_tombstones_; i++) {
    indexes.push_back(static_cast<int>(tombstones_[i]));
  }
  num_tombstones_ = 0;
  std::sort(indexes.begin(), indexes.end(), std::greater<>());
  for (auto index : indexes) {
    RemoveAt(index);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveEntriesTo(BPlusTreeLeafPage *that, int start) {
  const int n = GetSize();
  for (int i = start; i < n; i++) {
    that->InsertAt(that->GetSize(), key_array_[i], rid_array_[i]);
  }
  SetSize(start);
  num_tombstones_ = 0;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
