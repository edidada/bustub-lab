//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include <optional>
#include <utility>

#include "storage/index/index_iterator.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, page_id_t page_id, int index)
    : bpm_(std::move(bpm)), page_id_(page_id), index_(index) {
  if (page_id_ != INVALID_PAGE_ID) {
    guard_.emplace(bpm_->ReadPage(page_id_));
    SkipToValidEntry();
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID || !guard_.has_value(); }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf = guard_->template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return {leaf->KeyRefAt(index_), leaf->ValueRefAt(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SkipToValidEntry() {
  while (guard_.has_value()) {
    auto leaf = guard_->template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    while (index_ < leaf->GetSize() && leaf->IsTombstone(index_)) {
      index_++;
    }
    if (index_ < leaf->GetSize()) {
      return;
    }
    // Move on to the next leaf page.
    page_id_t next = leaf->GetNextPageId();
    guard_.reset();
    if (next == INVALID_PAGE_ID) {
      page_id_ = INVALID_PAGE_ID;
      index_ = 0;
      return;
    }
    page_id_ = next;
    index_ = 0;
    guard_.emplace(bpm_->ReadPage(next));
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  index_++;
  SkipToValidEntry();
  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (page_id_ == INVALID_PAGE_ID && itr.page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
