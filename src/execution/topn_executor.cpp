//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_executor.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace bustub {

/**
 * Construct a new TopNExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopN plan to be executed
 */
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopN */
void TopNExecutor::Init() {
  child_executor_->Init();
  entries_.clear();
  results_.clear();
  result_pos_ = 0;

  const auto &child_schema = child_executor_->GetOutputSchema();
  TupleComparator comparator(plan_->GetOrderBy());
  const size_t n = plan_->GetN();

  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      if (n == 0) {
        break;
      }
      SortEntry entry{GenerateSortKey(tuple, plan_->GetOrderBy(), child_schema), tuple};
      // `entries_` is a max-heap under the comparator, so its front is the worst kept entry.
      entries_.push_back(entry);
      std::push_heap(entries_.begin(), entries_.end(), comparator);
      if (entries_.size() > n) {
        std::pop_heap(entries_.begin(), entries_.end(), comparator);
        entries_.pop_back();
      }
    }
  }

  results_ = entries_;
  std::sort(results_.begin(), results_.end(), comparator);
}

/**
 * Yield the next tuple batch from the TopN.
 */
auto TopNExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (result_pos_ < results_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->emplace_back(results_[result_pos_].second);
    rid_batch->emplace_back(RID{});
    result_pos_++;
  }

  return !tuple_batch->empty();
}

auto TopNExecutor::GetNumInHeap() -> size_t { return entries_.size(); };

}  // namespace bustub
