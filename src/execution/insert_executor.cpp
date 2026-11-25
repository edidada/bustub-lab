//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  has_emitted_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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
  auto *lock_mgr = exec_ctx_->GetLockManager();

  int32_t count = 0;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      auto rid_opt = table_heap->InsertTuple(TupleMeta{0, false}, tuple, lock_mgr, txn, plan_->GetTableOid());
      if (!rid_opt.has_value()) {
        continue;
      }
      count++;
      auto rid = rid_opt.value();
      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto *index = index_info->index_.get();
        auto key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
        index->InsertEntry(key, rid, txn);
      }
    }
  }

  has_emitted_ = true;
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
  return true;
}

}  // namespace bustub
