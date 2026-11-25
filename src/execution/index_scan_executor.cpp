//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

namespace {

/** B+Tree index over a single integer / two integer / generic fixed-size key column(s). */
using IntegerKeyType4 = GenericKey<4>;
using BPlusTreeIndexForOneIntegerColumn = BPlusTreeIndex<IntegerKeyType4, RID, GenericComparator<4>>;

/** Walk the whole B+Tree and collect all RIDs in key order. */
template <typename BTreeIndexType>
auto CollectAllRids(Index *index, std::vector<RID> *rids) -> bool {
  auto *btree_index = dynamic_cast<BTreeIndexType *>(index);
  if (btree_index == nullptr) {
    return false;
  }
  for (auto iter = btree_index->GetBeginIterator(); !iter.IsEnd(); ++iter) {
    rids->emplace_back((*iter).second);
  }
  return true;
}

}  // namespace

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  rids_.clear();
  rid_pos_ = 0;

  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info_ != nullptr, "table not found");
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  BUSTUB_ASSERT(index_info != nullptr, "index not found");
  auto *index = index_info->index_.get();
  auto *txn = exec_ctx_->GetTransaction();

  if (!plan_->pred_keys_.empty()) {
    // Point lookup by the given key values.
    auto key_schema = index->GetKeySchema();
    std::vector<Value> key_values;
    key_values.reserve(plan_->pred_keys_.size());
    Tuple dummy;
    for (const auto &expr : plan_->pred_keys_) {
      key_values.emplace_back(expr->Evaluate(&dummy, *key_schema));
    }
    Tuple key(std::move(key_values), key_schema);
    index->ScanKey(key, &rids_, txn);
    return;
  }

  // Full index scan: walk the whole B+Tree and collect all RIDs in key order.
  if (CollectAllRids<BPlusTreeIndexForOneIntegerColumn>(index, &rids_) ||
      CollectAllRids<BPlusTreeIndexForTwoIntegerColumn>(index, &rids_) ||
      CollectAllRids<BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>>(index, &rids_) ||
      CollectAllRids<BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>>(index, &rids_) ||
      CollectAllRids<BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>>(index, &rids_)) {
    return;
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto *table_heap = table_info_->table_.get();
  while (rid_pos_ < rids_.size() && tuple_batch->size() < batch_size) {
    auto rid = rids_[rid_pos_++];
    auto [meta, tuple] = table_heap->GetTuple(rid);
    auto *txn = exec_ctx_->GetTransaction();
    std::optional<Tuple> visible;
    if (txn == nullptr) {
      if (!meta.is_deleted_) {
        visible = std::move(tuple);
      }
    } else {
      visible = GetTupleVisibleToTxn(exec_ctx_->GetTransactionManager(), table_heap, rid, &table_info_->schema_, txn);
    }
    if (!visible.has_value()) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto pred = plan_->filter_predicate_->Evaluate(&(*visible), plan_->OutputSchema());
      if (pred.IsNull() || pred.GetAs<int8_t>() != 1) {
        continue;
      }
    }
    tuple_batch->emplace_back(*visible);
    rid_batch->emplace_back(rid);
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
