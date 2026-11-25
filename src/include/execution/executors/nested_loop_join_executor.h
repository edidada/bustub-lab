//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** The left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** The right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** Buffer of left tuples for the current left batch */
  std::vector<Tuple> left_buffer_;

  /** Position of the next unconsumed left tuple in `left_buffer_` */
  size_t left_pos_{0};

  /** The left tuple currently being matched against the right side */
  std::optional<Tuple> current_left_;

  /** The current right tuples for `current_left_` */
  std::vector<Tuple> right_buffer_;

  /** Position of the next unconsumed right tuple in `right_buffer_` */
  size_t right_pos_{0};

  /** Whether `right_buffer_` is exhausted for the current left tuple */
  bool right_done_{true};

  /** Whether the current left tuple has produced at least one joined row */
  bool current_left_matched_{false};

};

}  // namespace bustub
