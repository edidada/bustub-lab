//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <optional>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

namespace {

/** @brief Index of the first entry whose key is >= `key` (tombstoned entries included). */
template <typename LeafPage, typename KeyType, typename KeyComparator>
auto LeafLowerBound(const LeafPage *leaf, const KeyType &key, const KeyComparator &comparator) -> int {
  int pos = 0;
  const int n = leaf->GetSize();
  while (pos < n && comparator(leaf->KeyAt(pos), key) < 0) {
    pos++;
  }
  return pos;
}

/** @brief Index of the child of an internal page whose subtree may contain `key`. */
template <typename InternalPage, typename KeyType, typename KeyComparator>
auto ChildIndex(const InternalPage *page, const KeyType &key, const KeyComparator &comparator) -> int {
  int index = 0;
  const int n = page->GetSize();
  for (int i = 1; i < n; i++) {
    if (comparator(key, page->KeyAt(i)) >= 0) {
      index = i;
    } else {
      break;
    }
  }
  return index;
}

}  // namespace

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  const page_id_t root = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  header_guard.Drop();
  if (root == INVALID_PAGE_ID) {
    return true;
  }
  ReadPageGuard guard = bpm_->ReadPage(root);
  auto page = guard.As<BPlusTreePage>();
  if (!page->IsLeafPage()) {
    return false;
  }
  return guard.As<LeafPage>()->GetSize() == 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;
  page_id_t cur;
  {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    cur = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  }
  if (cur == INVALID_PAGE_ID) {
    return false;
  }

  while (true) {
    ReadPageGuard guard = bpm_->ReadPage(cur);
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      auto leaf = guard.As<LeafPage>();
      const int pos = LeafLowerBound(leaf, key, comparator_);
      if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0 && !leaf->IsTombstone(pos)) {
        result->push_back(leaf->ValueAt(pos));
        return true;
      }
      return false;
    }
    auto internal = guard.As<InternalPage>();
    cur = internal->ValueAt(ChildIndex(internal, key, comparator_));
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  page_id_t root = header->root_page_id_;

  // Case 1: the tree is empty, create the root leaf page.
  if (root == INVALID_PAGE_ID) {
    const page_id_t new_pid = bpm_->NewPage();
    ctx.write_set_.emplace_back(bpm_->WritePage(new_pid));
    auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
    leaf->Init(leaf_max_size_);
    leaf->InsertAt(0, key, value);
    header->root_page_id_ = new_pid;
    return true;
  }

  // Case 2: descend to the target leaf, latching every page on the way down.
  page_id_t cur = root;
  while (true) {
    ctx.write_set_.emplace_back(bpm_->WritePage(cur));
    auto *page = ctx.write_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
      const int pos = LeafLowerBound(leaf, key, comparator_);
      if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0) {
        if (!leaf->IsTombstone(pos)) {
          return false;  // duplicate key
        }
        // Revive a logically deleted entry.
        leaf->ClearTombstone(pos);
        leaf->SetValueAt(pos, value);
        return true;
      }
      leaf->InsertAt(pos, key, value);
      break;
    }
    auto *internal = ctx.write_set_.back().template AsMut<InternalPage>();
    cur = internal->ValueAt(ChildIndex(internal, key, comparator_));
  }

  // Case 3: propagate splits upwards while a page overflows.
  for (int level = static_cast<int>(ctx.write_set_.size()) - 1; level >= 0; level--) {
    auto *page = ctx.write_set_[level].template As<BPlusTreePage>();
    if (page->GetSize() <= page->GetMaxSize()) {
      break;
    }

    const page_id_t old_pid = ctx.write_set_[level].GetPageId();
    const page_id_t new_pid = bpm_->NewPage();
    WritePageGuard new_guard = bpm_->WritePage(new_pid);
    KeyType separator;

    if (page->IsLeafPage()) {
      auto *leaf = ctx.write_set_[level].template AsMut<LeafPage>();
      leaf->PurgeTombstones();
      if (leaf->GetSize() <= leaf->GetMaxSize()) {
        break;  // purging the tombstones was enough
      }
      auto *new_leaf = new_guard.template AsMut<LeafPage>();
      new_leaf->Init(leaf_max_size_);
      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_pid);
      const int mid = leaf->GetSize() / 2;
      leaf->MoveEntriesTo(new_leaf, mid);
      separator = new_leaf->KeyAt(0);
    } else {
      auto *internal = ctx.write_set_[level].template AsMut<InternalPage>();
      auto *new_internal = new_guard.template AsMut<InternalPage>();
      new_internal->Init(internal_max_size_);
      const int mid = internal->GetSize() / 2;
      separator = internal->KeyAt(mid);
      for (int i = mid; i < internal->GetSize(); i++) {
        new_internal->InsertAt(new_internal->GetSize(), internal->KeyAt(i), internal->ValueAt(i));
      }
      internal->SetSize(mid);
    }

    if (level == 0) {
      // The root page split: create a new root.
      const page_id_t new_root_pid = bpm_->NewPage();
      WritePageGuard new_root_guard = bpm_->WritePage(new_root_pid);
      auto *new_root = new_root_guard.template AsMut<InternalPage>();
      new_root->Init(internal_max_size_);
      new_root->InsertAt(0, KeyType(), old_pid);
      new_root->InsertAt(1, separator, new_pid);
      header->root_page_id_ = new_root_pid;
        break;
    }

    auto *parent = ctx.write_set_[level - 1].template AsMut<InternalPage>();
    const int index = parent->ValueIndex(old_pid);
    parent->InsertAt(index + 1, separator, new_pid);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  page_id_t root = header->root_page_id_;
  if (root == INVALID_PAGE_ID) {
    return;
  }

  page_id_t cur = root;
  while (true) {
    ctx.write_set_.emplace_back(bpm_->WritePage(cur));
    auto *page = ctx.write_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto *internal = ctx.write_set_.back().template AsMut<InternalPage>();
    cur = internal->ValueAt(ChildIndex(internal, key, comparator_));
  }

  auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  const int pos = LeafLowerBound(leaf, key, comparator_);
  if (pos >= leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0 || leaf->IsTombstone(pos)) {
    return;
  }

  // Lazily delete the entry when removing it would underflow the leaf.
  if (LEAF_PAGE_TOMB_CNT > 0 && leaf->GetSize() <= leaf->GetMinSize() && leaf->CanAddTombstone()) {
    leaf->AddTombstone(pos);
    return;
  }
  leaf->RemoveAt(pos);

  // Pages that must be freed once all latches held by this operation are released.
  std::vector<page_id_t> pending_deletes;

  // Fix any underflow, from the leaf upwards.
  for (int level = static_cast<int>(ctx.write_set_.size()) - 1; level >= 1; level--) {
    auto *page = ctx.write_set_[level].template As<BPlusTreePage>();
    if (page->GetSize() >= page->GetMinSize()) {
      break;
    }
    auto *parent = ctx.write_set_[level - 1].template AsMut<InternalPage>();
    const int index = parent->ValueIndex(ctx.write_set_[level].GetPageId());
    const page_id_t left_pid = index > 0 ? parent->ValueAt(index - 1) : INVALID_PAGE_ID;
    const page_id_t right_pid = index + 1 < parent->GetSize() ? parent->ValueAt(index + 1) : INVALID_PAGE_ID;

    bool fixed = false;

    if (page->IsLeafPage()) {
      auto *leaf = ctx.write_set_[level].template AsMut<LeafPage>();
      if (left_pid != INVALID_PAGE_ID) {
        auto left_guard = bpm_->WritePage(left_pid);
        auto *left = left_guard.template AsMut<LeafPage>();
        left->PurgeTombstones();
        if (left->GetSize() > left->GetMinSize()) {
          const int last = left->GetSize() - 1;
          leaf->InsertAt(0, left->KeyAt(last), left->ValueAt(last));
          left->RemoveAt(last);
          parent->SetKeyAt(index, leaf->KeyAt(0));
          fixed = true;
        }
      }
      if (!fixed && right_pid != INVALID_PAGE_ID) {
        auto right_guard = bpm_->WritePage(right_pid);
        auto *right = right_guard.template AsMut<LeafPage>();
        right->PurgeTombstones();
        if (right->GetSize() > right->GetMinSize()) {
          leaf->InsertAt(leaf->GetSize(), right->KeyAt(0), right->ValueAt(0));
          right->RemoveAt(0);
          parent->SetKeyAt(index + 1, right->KeyAt(0));
          fixed = true;
        }
      }
      if (!fixed && right_pid != INVALID_PAGE_ID) {
        // Merge the right sibling into the current leaf.
        const page_id_t victim = right_pid;
        {
          auto right_guard = bpm_->WritePage(victim);
          auto *right = right_guard.template AsMut<LeafPage>();
          right->PurgeTombstones();
          right->MoveEntriesTo(leaf, 0);
          leaf->SetNextPageId(right->GetNextPageId());
        }
        parent->RemoveAt(index + 1);
        pending_deletes.push_back(victim);
        fixed = true;
      }
      if (!fixed && left_pid != INVALID_PAGE_ID) {
        // Merge the current leaf into the left sibling.
        const page_id_t victim = ctx.write_set_[level].GetPageId();
        const page_id_t next_of_victim = leaf->GetNextPageId();
        {
          auto left_guard = bpm_->WritePage(left_pid);
          auto *left = left_guard.template AsMut<LeafPage>();
          left->PurgeTombstones();
          leaf->PurgeTombstones();
          leaf->MoveEntriesTo(left, 0);
          left->SetNextPageId(next_of_victim);
        }
        parent->RemoveAt(index);
        pending_deletes.push_back(victim);
        fixed = true;
      }
    } else {
      auto *internal = ctx.write_set_[level].template AsMut<InternalPage>();
      if (left_pid != INVALID_PAGE_ID) {
        auto left_guard = bpm_->WritePage(left_pid);
        auto *left = left_guard.template AsMut<InternalPage>();
        if (left->GetSize() > left->GetMinSize()) {
          const int last = left->GetSize() - 1;
          const KeyType separator = parent->KeyAt(index);
          internal->InsertAt(0, separator, left->ValueAt(last));
          internal->SetKeyAt(1, separator);
          parent->SetKeyAt(index, left->KeyAt(last));
          left->RemoveAt(last);
          fixed = true;
        }
      }
      if (!fixed && right_pid != INVALID_PAGE_ID) {
        auto right_guard = bpm_->WritePage(right_pid);
        auto *right = right_guard.template AsMut<InternalPage>();
        if (right->GetSize() > right->GetMinSize()) {
          const KeyType separator = parent->KeyAt(index + 1);
          internal->InsertAt(internal->GetSize(), separator, right->ValueAt(0));
          parent->SetKeyAt(index + 1, right->KeyAt(1));
          right->RemoveAt(0);
          fixed = true;
        }
      }
      if (!fixed && right_pid != INVALID_PAGE_ID) {
        const page_id_t victim = right_pid;
        {
          auto right_guard = bpm_->WritePage(victim);
          auto *right = right_guard.template AsMut<InternalPage>();
          internal->InsertAt(internal->GetSize(), parent->KeyAt(index + 1), right->ValueAt(0));
          for (int i = 1; i < right->GetSize(); i++) {
            internal->InsertAt(internal->GetSize(), right->KeyAt(i), right->ValueAt(i));
          }
        }
        parent->RemoveAt(index + 1);
        pending_deletes.push_back(victim);
        fixed = true;
      }
      if (!fixed && left_pid != INVALID_PAGE_ID) {
        const page_id_t victim = ctx.write_set_[level].GetPageId();
        {
          auto left_guard = bpm_->WritePage(left_pid);
          auto *left = left_guard.template AsMut<InternalPage>();
          for (int i = 0; i < internal->GetSize(); i++) {
            const KeyType key = i == 0 ? parent->KeyAt(index) : internal->KeyAt(i);
            left->InsertAt(left->GetSize(), key, internal->ValueAt(i));
          }
        }
        parent->RemoveAt(index);
        ctx.write_set_[level].Drop();
        bpm_->DeletePage(victim);
        fixed = true;
      }
    }

    if (!fixed) {
      break;
    }
  }

  // Shrink the tree if the root internal page has only one child left.
  size_t root_pos = 0;
  while (root_pos < ctx.write_set_.size()) {
    auto *page = ctx.write_set_[root_pos].template As<BPlusTreePage>();
    if (page->IsLeafPage() || page->GetSize() > 1) {
      break;
    }
    const page_id_t old_root = ctx.write_set_[root_pos].GetPageId();
    const page_id_t child = ctx.write_set_[root_pos].template As<InternalPage>()->ValueAt(0);
    header->root_page_id_ = child;
    pending_deletes.push_back(old_root);
    root_pos++;
  }

  // An emptied root leaf means the tree has no keys at all.
  if (root_pos < ctx.write_set_.size()) {
    auto *page = ctx.write_set_[root_pos].template As<BPlusTreePage>();
    if (page->IsLeafPage() && page->GetSize() == 0) {
      pending_deletes.push_back(ctx.write_set_[root_pos].GetPageId());
      header->root_page_id_ = INVALID_PAGE_ID;
    }
  }

  // Release every latch before freeing pages.
  ctx.write_set_.clear();
  for (const page_id_t victim : pending_deletes) {
    bpm_->DeletePage(victim);
  }
}
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t cur;
  {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    cur = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  }
  if (cur == INVALID_PAGE_ID) {
    return End();
  }
  while (true) {
    ReadPageGuard guard = bpm_->ReadPage(cur);
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      guard.Drop();
      return INDEXITERATOR_TYPE(bpm_, cur, 0);
    }
    cur = guard.As<InternalPage>()->ValueAt(0);
  }
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t cur;
  {
    ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
    cur = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  }
  if (cur == INVALID_PAGE_ID) {
    return End();
  }
  while (true) {
    ReadPageGuard guard = bpm_->ReadPage(cur);
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      int index = LeafLowerBound(guard.As<LeafPage>(), key, comparator_);
      guard.Drop();
      return INDEXITERATOR_TYPE(bpm_, cur, index);
    }
    cur = guard.As<InternalPage>()->ValueAt(ChildIndex(guard.As<InternalPage>(), key, comparator_));
  }
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  return header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
