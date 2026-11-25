//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/update_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  has_emitted_ = false;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  BUSTUB_ASSERT(table_info_ != nullptr, "table not found");
}

/**
 * Yield the number of rows updated in the table.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (has_emitted_) {
    return false;
  }

  auto *table_heap = table_info_->table_.get();
  auto *txn = exec_ctx_->GetTransaction();
  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto child_schema = child_executor_->GetOutputSchema();

  int32_t count = 0;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  std::vector<std::pair<RID, Tuple>> targets;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (size_t i = 0; i < tuples.size(); i++) {
      std::vector<Value> values;
      values.reserve(plan_->target_expressions_.size());
      for (const auto &expr : plan_->target_expressions_) {
        values.emplace_back(expr->Evaluate(&tuples[i], child_schema));
      }
      targets.emplace_back(rids[i], Tuple(std::move(values), &table_info_->schema_));
    }
  }

  for (const auto &[rid, new_tuple] : targets) {
    auto [old_meta, old_tuple] = table_heap->GetTuple(rid);
    if (old_meta.is_deleted_) {
      continue;
    }
    table_heap->UpdateTupleMeta(TupleMeta{old_meta.ts_, true}, rid);
    for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto *index = index_info->index_.get();
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(old_key, rid, txn);
    }

    auto new_rid = table_heap->InsertTuple(TupleMeta{0, false}, new_tuple, lock_mgr, txn, plan_->GetTableOid());
    if (new_rid.has_value()) {
      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *index = index_info->index_.get();
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
        index->InsertEntry(new_key, new_rid.value(), txn);
      }
    }
    count++;
  }

  has_emitted_ = true;
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
  return true;
}

}  // namespace bustub
