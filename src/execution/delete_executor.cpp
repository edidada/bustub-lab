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
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
      targets.emplace_back(rids[i], tuple);
    }
  }

  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  for (const auto &[rid, tuple] : targets) {
    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);
    if (txn != nullptr && IsWriteWriteConflict(base_meta, undo_link.value_or(UndoLink{}), txn->GetTransactionTempTs(),
                                               txn->GetReadTs())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict on delete");
    }
    if (base_meta.is_deleted_) {
      continue;  // already deleted by this txn
    }

    for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
      auto *index = index_info->index_.get();
      // Under MVCC the index entry has to stay: older snapshots may still need to find this tuple.
      (void)index;
    }

    if (txn == nullptr) {
      table_heap->UpdateTupleMeta(TupleMeta{base_meta.ts_, true}, rid);
    } else {
      ModifyTuple(txn_mgr, txn, table_heap, plan_->GetTableOid(), rid, &table_info->schema_, base_tuple, base_meta,
                  undo_link, nullptr);
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
