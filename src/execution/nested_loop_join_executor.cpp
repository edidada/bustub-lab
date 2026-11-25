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
  left_buffer_.clear();
  left_pos_ = 0;
  current_left_.reset();
  right_buffer_.clear();
  right_pos_ = 0;
  right_done_ = true;
  current_left_matched_ = false;

  left_executor_->Init();
}

/**
 * Yield the next tuple batch from the join.
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  const auto &predicate = plan_->Predicate();
  const bool is_left_join = plan_->GetJoinType() == JoinType::LEFT;

  auto emit_joined = [&](const Tuple &left_tuple, const Tuple *right_tuple) {
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
    // Fetch the next left tuple, re-initializing the right child for every left tuple.
    if (!current_left_.has_value()) {
      if (left_pos_ >= left_buffer_.size()) {
        std::vector<Tuple> tuples;
        std::vector<RID> rids;
        if (!left_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
          return !tuple_batch->empty();
        }
        left_buffer_ = std::move(tuples);
        left_pos_ = 0;
        if (left_buffer_.empty()) {
          continue;
        }
      }
      current_left_ = left_buffer_[left_pos_++];
      right_executor_->Init();
      right_buffer_.clear();
      right_pos_ = 0;
      right_done_ = false;
      current_left_matched_ = false;
    }

    // Pull the next right tuple for the current left tuple.
    if (!right_done_ && right_pos_ >= right_buffer_.size()) {
      std::vector<Tuple> tuples;
      std::vector<RID> rids;
      if (right_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
        right_buffer_ = std::move(tuples);
        right_pos_ = 0;
      } else {
        right_done_ = true;
      }
    }

    if (!right_done_) {
      const auto &right_tuple = right_buffer_[right_pos_++];
      bool ok = true;
      if (predicate != nullptr) {
        auto value = predicate->EvaluateJoin(&*current_left_, left_schema, &right_tuple, right_schema);
        ok = !value.IsNull() && value.GetAs<int8_t>() == 1;
      }
      if (ok) {
        current_left_matched_ = true;
        emit_joined(*current_left_, &right_tuple);
      }
      continue;
    }

    // Right side exhausted for this left tuple: emit a null-padded row for a left join without matches.
    if (!current_left_matched_ && is_left_join) {
      current_left_matched_ = true;
      emit_joined(*current_left_, nullptr);
      continue;
    }

    current_left_.reset();
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
