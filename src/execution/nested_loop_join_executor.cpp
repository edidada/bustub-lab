//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  results_.clear();
  result_pos_ = 0;

  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> tuples;
  std::vector<RID> rids;

  std::vector<Tuple> left_tuples;
  while (left_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      left_tuples.emplace_back(tuple);
    }
  }

  std::vector<Tuple> right_tuples;
  while (right_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      right_tuples.emplace_back(tuple);
    }
  }

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  const auto &predicate = plan_->Predicate();

  for (const auto &left_tuple : left_tuples) {
    bool matched = false;
    for (const auto &right_tuple : right_tuples) {
      bool ok = true;
      if (predicate != nullptr) {
        auto value = predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        ok = !value.IsNull() && value.GetAs<int8_t>() == 1;
      }
      if (ok) {
        matched = true;
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_schema, i));
        }
        results_.emplace_back(Tuple(std::move(values), &GetOutputSchema()));
      }
    }
    if (!matched && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (const auto &column : right_schema.GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
      }
      results_.emplace_back(Tuple(std::move(values), &GetOutputSchema()));
    }
  }
}

/**
 * Yield the next tuple batch from the join.
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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
