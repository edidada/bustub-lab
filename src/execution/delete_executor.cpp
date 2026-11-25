//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  has_emitted_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (has_emitted_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "table not found");
  auto *table_heap = table_info->table_.get();
  auto *txn = exec_ctx_->GetTransaction();

  int32_t count = 0;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  std::vector<std::pair<RID, Tuple>> targets;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (size_t i = 0; i < rids.size(); i++) {
      auto [meta, tuple] = table_heap->GetTuple(rids[i]);
      if (meta.is_deleted_) {
        continue;
      }
      targets.emplace_back(rids[i], tuple);
    }
  }

  for (const auto &[rid, tuple] : targets) {
    auto meta = table_heap->GetTupleMeta(rid);
    if (meta.is_deleted_) {
      continue;
    }
    table_heap->UpdateTupleMeta(TupleMeta{meta.ts_, true}, rid);
    count++;
    for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
      auto *index = index_info->index_.get();
      auto key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(key, rid, txn);
    }
  }

  has_emitted_ = true;
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
  return true;
}

}  // namespace bustub
