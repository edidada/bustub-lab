//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

auto GenerateNullTuple(const Schema *schema) -> Tuple {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
  }
  return Tuple{values, schema};
}

/** Walk the version chain of a RID and collect the undo logs owned by `txn`, newest first. */
auto CollectMyUndoLogs(TransactionManager *txn_mgr, RID rid, Transaction *txn) -> std::vector<UndoLog> {
  std::vector<UndoLog> logs;
  auto link = txn_mgr->GetUndoLink(rid);
  while (link.has_value() && link->IsValid() && link->prev_txn_ == txn->GetTransactionId()) {
    auto log = txn_mgr->GetUndoLogOptional(*link);
    if (!log.has_value()) {
      break;
    }
    logs.push_back(*log);
    link = log->prev_version_;
  }
  return logs;
}

}  // namespace

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  (void)txn;
  return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // Turn every tuple written by this txn from its temporary timestamp into the commit timestamp.
  const auto temp_ts = txn->GetTransactionTempTs();
  for (const auto &[oid, rids] : txn->GetWriteSet()->GetWriteSets()) {
    auto table_info = catalog_->GetTable(oid);
    if (table_info == nullptr) {
      continue;
    }
    auto table_heap = table_info->table_.get();
    for (const auto &rid : rids) {
      auto page = table_heap->AcquireTablePageWriteLock(rid);
      auto meta = table_heap->GetTupleMetaWithLockAcquired(rid, page.As<TablePage>());
      if (meta.ts_ != temp_ts) {
        continue;  // somebody else already took care of this slot
      }
      auto tuple = table_heap->GetTupleWithLockAcquired(rid, page.As<TablePage>()).second;
      meta.ts_ = commit_ts;
      table_heap->UpdateTupleInPlaceWithLockAcquired(meta, tuple, rid, page.As<TablePage>());
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  txn->commit_ts_ = commit_ts;
  last_commit_ts_.store(commit_ts);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction * txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  const auto temp_ts = txn->GetTransactionTempTs();
  for (const auto &[oid, rids] : txn->GetWriteSet()->GetWriteSets()) {
    auto table_info = catalog_->GetTable(oid);
    if (table_info == nullptr) {
      continue;
    }
    auto table_heap = table_info->table_.get();
    const auto &schema = &table_info->schema_;
    for (const auto &rid : rids) {
      auto my_logs = CollectMyUndoLogs(this, rid, txn);
      auto page = table_heap->AcquireTablePageWriteLock(rid);
      auto [meta, tuple] = table_heap->GetTupleWithLockAcquired(rid, page.As<TablePage>());
      if (meta.ts_ != temp_ts) {
        continue;
      }
      if (my_logs.empty()) {
        // The tuple was inserted by this txn: undo the insert by marking it deleted.
        meta.is_deleted_ = true;
        auto null_tuple = GenerateNullTuple(schema);
        table_heap->UpdateTupleInPlaceWithLockAcquired(meta, null_tuple, rid, page.As<TablePage>());
        continue;
      }
      // Roll the tuple back to the state before this txn touched it.
      auto prev_tuple = ReconstructTuple(schema, tuple, meta, my_logs);
      auto oldest = my_logs.back();
      TupleMeta restored_meta{oldest.ts_, oldest.is_deleted_};
      if (prev_tuple.has_value()) {
        table_heap->UpdateTupleInPlaceWithLockAcquired(restored_meta, *prev_tuple, rid, page.As<TablePage>());
      } else {
        restored_meta.is_deleted_ = true;
        auto null_tuple = GenerateNullTuple(schema);
        table_heap->UpdateTupleInPlaceWithLockAcquired(restored_meta, null_tuple, rid, page.As<TablePage>());
      }
      UpdateUndoLink(rid, oldest.prev_version_, nullptr);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  const auto watermark = GetWatermark();

  // Step 1: cut the version chains at the watermark, and remember which txns are still referenced.
  std::unordered_set<txn_id_t> referenced_txns;
  for (const auto &table_name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
      continue;
    }
    auto *table_heap = table_info->table_.get();
    auto iter = table_heap->MakeEagerIterator();
    while (!iter.IsEnd()) {
      auto rid = iter.GetRID();
      ++iter;
      auto link = GetUndoLink(rid);
      while (link.has_value() && link->IsValid()) {
        auto log = GetUndoLogOptional(*link);
        if (!log.has_value()) {
          break;
        }
        if (log->ts_ < watermark) {
          // Everything from here on is older than the watermark: it can never be read again.
          UpdateUndoLink(rid, std::nullopt, nullptr);
          break;
        }
        referenced_txns.insert(link->prev_txn_);
        link = log->prev_version_;
      }
    }
  }

  // Step 2: drop committed / aborted txns that nobody can reference any more.
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto &txn = it->second;
    if (txn->state_ == TransactionState::RUNNING || txn->state_ == TransactionState::TAINTED ||
        referenced_txns.find(txn->GetTransactionId()) != referenced_txns.end()) {
      ++it;
      continue;
    }
    if (txn->commit_ts_ >= watermark) {
      ++it;
      continue;
    }
    it = txn_map_.erase(it);
  }
}

}  // namespace bustub
