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
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  (void)lock_mgr;

  int32_t count = 0;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      const timestamp_t txn_temp_ts = txn == nullptr ? 0 : txn->GetTransactionTempTs();

      // If every index key of this tuple already points at a *tombstone* slot, reuse that slot instead
      // of appending a new one (this is how re-inserting a previously deleted key keeps working).
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      std::optional<RID> reuse_rid;
      bool can_reuse = txn != nullptr && !indexes.empty();
      for (const auto &index_info : indexes) {
        if (!can_reuse) {
          break;
        }
        auto key = tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        std::vector<RID> existing;
        index_info->index_->ScanKey(key, &existing, txn);
        if (existing.size() != 1) {
          can_reuse = false;  // no entry, or ambiguous: fall back to a normal insert
          if (!existing.empty() && index_info->is_primary_key_) {
            txn->SetTainted();
            throw ExecutionException("duplicate primary key");
          }
          break;
        }
        auto rid = existing[0];
        auto meta = table_heap->GetTupleMeta(rid);
        if (!meta.is_deleted_ || (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn_temp_ts)) {
          can_reuse = false;
          if (index_info->is_primary_key_) {
            txn->SetTainted();
            throw ExecutionException("duplicate primary key");
          }
          break;
        }
        if (reuse_rid.has_value() && !((*reuse_rid) == rid)) {
          can_reuse = false;
          break;
        }
        reuse_rid = rid;
      }
      if (reuse_rid.has_value()) {
        auto rid = *reuse_rid;
        auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_heap, rid);
        auto log = UndoLog{true, std::vector<bool>(table_info->schema_.GetColumnCount(), false), Tuple{},
                           base_meta.ts_, undo_link.value_or(UndoLink{})};
        auto link = txn->AppendUndoLog(log);
        UpdateTupleAndUndoLink(exec_ctx_->GetTransactionManager(), rid, link, table_heap, txn,
                               TupleMeta{txn_temp_ts, false}, tuple);
        txn->AppendWriteSet(plan_->GetTableOid(), rid);
        count++;
        continue;  // the primary key index entry already points at this slot
      }

      auto rid_opt =
          table_heap->InsertTuple(TupleMeta{txn_temp_ts, false}, tuple, lock_mgr, txn, plan_->GetTableOid());
      if (!rid_opt.has_value()) {
        continue;
      }
      count++;
      auto rid = rid_opt.value();
      if (txn != nullptr) {
        txn->AppendWriteSet(plan_->GetTableOid(), rid);
      }

      bool pk_conflict = false;
      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto *index = index_info->index_.get();
        auto key = tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        if (!index->InsertEntry(key, rid, txn) && index_info->is_primary_key_) {
          pk_conflict = true;
        }
      }
      if (pk_conflict) {
        table_heap->UpdateTupleMeta(TupleMeta{txn_temp_ts, true}, rid);
        if (txn != nullptr) {
          txn->SetTainted();
          throw ExecutionException("duplicate primary key");
        }
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
