//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  hash_table_.clear();
  left_tuples_.clear();
  matches_.clear();
  left_pos_ = 0;
  match_pos_ = 0;
  left_tuple_.reset();

  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> tuples;
  std::vector<RID> rids;

  const auto &right_schema = right_executor_->GetOutputSchema();

  // Build phase: hash the right (inner) side.
  auto build_key = [&](const Tuple &tuple) {
    HashJoinKey key;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      key.values_.emplace_back(expr->Evaluate(&tuple, right_schema));
    }
    return key;
  };
  while (right_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      hash_table_[build_key(tuple)].emplace_back(tuple);
    }
  }

  // Probe phase input: materialize the left (outer) side.
  while (left_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      left_tuples_.emplace_back(tuple);
    }
  }
}

/**
 * Yield the next tuple batch from the hash join.
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  auto emit = [&](const Tuple &left_tuple, const Tuple *right_tuple) {
    std::vector<Value> values;
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
      values.emplace_back(left_tuple.GetValue(&left_schema, i));
    }
    if (right_tuple != nullptr) {
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.emplace_back(right_tuple->GetValue(&right_schema, i));
      }
    } else {
      for (const auto &column : right_schema.GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
      }
    }
    tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
    rid_batch->emplace_back(RID{});
  };

  while (tuple_batch->size() < batch_size) {
    if (match_pos_ < matches_.size()) {
      emit(*left_tuple_, &matches_[match_pos_]);
      match_pos_++;
      continue;
    }
    if (left_pos_ >= left_tuples_.size()) {
      break;
    }

    left_tuple_ = left_tuples_[left_pos_];
    left_pos_++;
    matches_.clear();
    match_pos_ = 0;

    HashJoinKey key;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.values_.emplace_back(expr->Evaluate(&*left_tuple_, left_schema));
    }
    auto iter = hash_table_.find(key);
    if (iter != hash_table_.end()) {
      matches_ = iter->second;
    }

    if (matches_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      emit(*left_tuple_, nullptr);
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
