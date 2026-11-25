//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;
  const auto &key_b = entry_b.first;
  BUSTUB_ASSERT(key_a.size() == order_bys_.size(), "sort key size mismatch");
  BUSTUB_ASSERT(key_b.size() == order_bys_.size(), "sort key size mismatch");

  for (size_t i = 0; i < order_bys_.size(); i++) {
    const auto &value_a = key_a[i];
    const auto &value_b = key_b[i];
    const auto order_type = std::get<0>(order_bys_[i]);
    const auto null_type = std::get<1>(order_bys_[i]);
    if (value_a.CompareExactlyEquals(value_b)) {
      continue;
    }
    if (value_a.IsNull() || value_b.IsNull()) {
      bool null_first = false;
      switch (null_type) {
        case OrderByNullType::NULLS_FIRST:
          null_first = true;
          break;
        case OrderByNullType::NULLS_LAST:
          null_first = false;
          break;
        default:
          null_first = order_type != OrderByType::DESC;
          break;
      }
      return value_a.IsNull() ? null_first : !null_first;
    }
    bool less = value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
    if (order_type == OrderByType::DESC) {
      return !less;
    }
    return less;
  }
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());
  for (const auto &order_by : order_bys) {
    key.emplace_back(std::get<2>(order_by)->Evaluate(&tuple, schema));
  }
  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
namespace {

/** Build the (partial) schema of the columns a given undo log recorded. */
auto GetUndoLogSchema(const Schema *schema, const std::vector<bool> &modified_fields) -> Schema {
  std::vector<uint32_t> attrs;
  attrs.reserve(modified_fields.size());
  for (uint32_t i = 0; i < modified_fields.size(); i++) {
    if (modified_fields[i]) {
      attrs.push_back(i);
    }
  }
  return Schema::CopySchema(schema, attrs);
}

/** Build a tuple filled with NULL for every column of `schema`. */
auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
  }
  return Tuple{values, schema};
}

}  // namespace

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  bool deleted = base_meta.is_deleted_;
  auto reconstructed = base_meta.is_deleted_ ? GenerateNullTupleForSchema(schema) : base_tuple;

  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      deleted = true;
      continue;
    }
    deleted = false;
    auto log_schema = GetUndoLogSchema(schema, log.modified_fields_);
    std::vector<Value> values;
    values.reserve(schema->GetColumnCount());
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      values.push_back(reconstructed.GetValue(schema, i));
    }
    uint32_t log_idx = 0;
    for (uint32_t i = 0; i < schema->GetColumnCount() && i < log.modified_fields_.size(); i++) {
      if (!log.modified_fields_[i]) {
        continue;
      }
      values[i] = log.tuple_.GetValue(&log_schema, log_idx);
      log_idx++;
    }
    reconstructed = Tuple{values, schema};
  }

  if (deleted) {
    return std::nullopt;
  }
  return reconstructed;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  (void)rid;
  (void)base_tuple;
  std::vector<UndoLog> result;
  const auto read_ts = txn->GetReadTs();
  const auto temp_ts = txn->GetTransactionTempTs();
  auto current_ts = base_meta.ts_;

  // The tuple written by this txn itself (or a committed version visible to its snapshot) needs no undo log.
  if (current_ts == temp_ts || current_ts <= read_ts) {
    return result;
  }

  while (current_ts > read_ts) {
    if (!undo_link.has_value() || !undo_link->IsValid()) {
      return std::nullopt;  // the tuple did not exist at the time of the snapshot
    }
    auto log = txn_mgr->GetUndoLogOptional(*undo_link);
    if (!log.has_value()) {
      return std::nullopt;
    }
    result.push_back(*log);
    current_ts = log->ts_;
    undo_link = log->prev_version_;
  }

  return result;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  std::vector<Value> values;

  if (target_tuple == nullptr) {
    // A deletion only needs to record "this version was deleted".
    return UndoLog{true, modified_fields, Tuple{}, ts, prev_version};
  }

  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto old_value = base_tuple == nullptr ? ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType())
                                           : base_tuple->GetValue(schema, i);
    auto new_value = target_tuple->GetValue(schema, i);
    bool changed = base_tuple == nullptr || old_value.IsNull() != new_value.IsNull() ||
                   (old_value.IsNull() ? !new_value.IsNull()
                                       : (new_value.IsNull() ? !old_value.IsNull()
                                                             : old_value.CompareEquals(new_value) != CmpBool::CmpTrue));
    if (changed) {
      modified_fields[i] = true;
      values.push_back(old_value);
    }
  }

  auto log_schema = GetUndoLogSchema(schema, modified_fields);
  return UndoLog{false, modified_fields, Tuple{values, &log_schema}, ts, prev_version};
}

auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  if (log.is_deleted_) {
    return log;
  }

  auto modified_fields = log.modified_fields_;
  auto old_schema = GetUndoLogSchema(schema, log.modified_fields_);
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());

  uint32_t old_idx = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (i < modified_fields.size() && modified_fields[i]) {
      values.push_back(old_idx < old_schema.GetColumnCount() ? log.tuple_.GetValue(&old_schema, old_idx)
                                                             : ValueFactory::GetNullValueByType(
                                                                   schema->GetColumn(i).GetType()));
      old_idx++;
      continue;
    }
    if (target_tuple != nullptr) {
      auto old_value = base_tuple == nullptr ? ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType())
                                             : base_tuple->GetValue(schema, i);
      auto new_value = target_tuple->GetValue(schema, i);
      if (base_tuple == nullptr || old_value.IsNull() != new_value.IsNull() ||
          (!old_value.IsNull() && !new_value.IsNull()
               ? old_value.CompareEquals(new_value) != CmpBool::CmpTrue
               : false)) {
        if (i >= modified_fields.size()) {
          modified_fields.resize(schema->GetColumnCount(), false);
        }
        modified_fields[i] = true;
        values.push_back(old_value);
        continue;
      }
    }
    if (i >= modified_fields.size()) {
      modified_fields.resize(schema->GetColumnCount(), false);
    }
  }

  auto log_schema = GetUndoLogSchema(schema, modified_fields);
  return UndoLog{false, modified_fields, Tuple{values, &log_schema}, log.ts_, log.prev_version_};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
