//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "table not found");
  table_heap_ = table_info_->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_heap_->MakeEagerIterator());
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (!iterator_->IsEnd() && tuple_batch->size() < batch_size) {
    auto [meta, tuple] = iterator_->GetTuple();
    auto rid = iterator_->GetRID();
    ++(*iterator_);

    auto *txn = exec_ctx_->GetTransaction();
    std::optional<Tuple> visible;
    if (txn == nullptr) {
      if (!meta.is_deleted_) {
        visible = tuple;
      }
    } else {
      visible = GetTupleVisibleToTxn(exec_ctx_->GetTransactionManager(), table_heap_, rid, &table_info_->schema_, txn);
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
