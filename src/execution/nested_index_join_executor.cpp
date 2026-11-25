//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  results_.clear();
  result_pos_ = 0;

  child_executor_->Init();

  auto catalog = exec_ctx_->GetCatalog();
  auto inner_table_info = catalog->GetTable(plan_->GetInnerTableOid());
  BUSTUB_ASSERT(inner_table_info != nullptr, "inner table not found");
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  BUSTUB_ASSERT(index_info != nullptr, "index not found");
  auto *index = index_info->index_.get();
  auto *txn = exec_ctx_->GetTransaction();

  const auto &outer_schema = child_executor_->GetOutputSchema();
  const auto &inner_schema = inner_table_info->schema_;

  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &outer_tuple : tuples) {
      auto key_value = plan_->KeyPredicate()->Evaluate(&outer_tuple, outer_schema);

      std::vector<Value> key_values{key_value};
      Tuple key(std::move(key_values), index->GetKeySchema());

      std::vector<RID> matched_rids;
      index->ScanKey(key, &matched_rids, txn);

      bool has_match = false;
      for (const auto &rid : matched_rids) {
        auto [meta, inner_tuple] = inner_table_info->table_->GetTuple(rid);
        if (meta.is_deleted_) {
          continue;
        }
        has_match = true;
        std::vector<Value> values;
        for (uint32_t i = 0; i < outer_schema.GetColumnCount(); i++) {
          values.emplace_back(outer_tuple.GetValue(&outer_schema, i));
        }
        for (uint32_t i = 0; i < inner_schema.GetColumnCount(); i++) {
          values.emplace_back(inner_tuple.GetValue(&inner_schema, i));
        }
        results_.emplace_back(Tuple(std::move(values), &GetOutputSchema()));
      }

      if (!has_match && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < outer_schema.GetColumnCount(); i++) {
          values.emplace_back(outer_tuple.GetValue(&outer_schema, i));
        }
        for (const auto &column : inner_schema.GetColumns()) {
          values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
        }
        results_.emplace_back(Tuple(std::move(values), &GetOutputSchema()));
      }
    }
  }
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (result_pos_ < results_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->emplace_back(results_[result_pos_++]);
    rid_batch->emplace_back(RID{});
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
