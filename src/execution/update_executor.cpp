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
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // Pass 1: conflict check + drop the old index entries (all of them, before inserting new ones,
  // otherwise primary-key updates that shift keys collide with each other).
  struct UpdateTarget {
    RID rid_;
    Tuple new_tuple_;
    TupleMeta base_meta_;
    Tuple base_tuple_;
    std::optional<UndoLink> undo_link_;
  };
  std::vector<UpdateTarget> live;
  for (const auto &[rid, new_tuple] : targets) {
    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);
    if (txn != nullptr && IsWriteWriteConflict(base_meta, undo_link.value_or(UndoLink{}), txn->GetTransactionTempTs(),
                                               txn->GetReadTs())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict on update");
    }
    if (base_meta.is_deleted_) {
      continue;
    }
    live.push_back(UpdateTarget{rid, new_tuple, base_meta, base_tuple, undo_link});
    for (const auto &index_info : indexes) {
      auto *index = index_info->index_.get();
      auto old_key = base_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->DeleteEntry(old_key, rid, txn);
    }
  }

  // Pass 2: write the new versions. The base snapshot comes from pass 1, so the compare-and-swap in
  // ModifyTuple() fails if another txn slipped in between the two passes.
  for (const auto &target : live) {
    auto rid = target.rid_;
    if (txn == nullptr) {
      table_heap->UpdateTupleInPlace(TupleMeta{target.base_meta_.ts_, false}, target.new_tuple_, rid);
    } else {
      if (!ModifyTuple(txn_mgr, txn, table_heap, plan_->GetTableOid(), rid, &table_info_->schema_,
                       target.base_tuple_, target.base_meta_, target.undo_link_, &target.new_tuple_)) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict on update");
      }
    }
    count++;
  }

  // Pass 3: add the new index entries.
  for (const auto &target : live) {
    auto rid = target.rid_;
    for (const auto &index_info : indexes) {
      auto *index = index_info->index_.get();
      auto new_key = target.new_tuple_.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->InsertEntry(new_key, rid, txn);
    }
  }

  has_emitted_ = true;
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
  return true;
}

}  // namespace bustub
