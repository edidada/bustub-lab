//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.cpp
//
// Identification: src/execution/sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/sort_executor.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SortExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sort plan to be executed
 */
SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the sort */
void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  next_idx_ = 0;

  const auto &child_schema = child_executor_->GetOutputSchema();
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      sorted_tuples_.emplace_back(GenerateSortKey(tuple, plan_->GetOrderBy(), child_schema), tuple);
    }
  }

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), TupleComparator(plan_->GetOrderBy()));
}

/**
 * Yield the next tuple batch from the sort.
 */
auto SortExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (next_idx_ < sorted_tuples_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->emplace_back(sorted_tuples_[next_idx_].second);
    rid_batch->emplace_back(RID{});
    next_idx_++;
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
