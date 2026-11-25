//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  result_run_.reset();
  result_iter_.reset();

  const auto &child_schema = child_executor_->GetOutputSchema();

  // Phase 1: read the input in chunks, sort each chunk in memory, and write it out as a run.
  constexpr size_t MEMORY_ENTRIES = 1024;
  std::vector<SortEntry> buffer;
  buffer.reserve(MEMORY_ENTRIES);
  std::vector<MergeSortRun> runs;

  auto flush_buffer = [&]() {
    if (buffer.empty()) {
      return;
    }
    std::sort(buffer.begin(), buffer.end(), cmp_);
    runs.emplace_back(WriteRun(buffer));
    buffer.clear();
  };

  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      buffer.emplace_back(GenerateSortKey(tuple, plan_->GetOrderBy(), child_schema), tuple);
      if (buffer.size() >= MEMORY_ENTRIES) {
        flush_buffer();
      }
    }
  }
  flush_buffer();

  // Phase 2: K-way merge the runs until only one is left.
  while (runs.size() > 1) {
    std::vector<MergeSortRun> merged;
    merged.reserve((runs.size() + K - 1) / K);
    for (size_t i = 0; i < runs.size(); i += K) {
      MergeSortRun accumulator = std::move(runs[i]);
      for (size_t j = i + 1; j < std::min(i + K, runs.size()); j++) {
        accumulator = MergeTwoRuns(accumulator, runs[j]);
      }
      merged.emplace_back(std::move(accumulator));
    }
    runs = std::move(merged);
  }

  if (!runs.empty()) {
    result_run_ = std::move(runs[0]);
    result_iter_ = result_run_->Begin();
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::WriteRun(const std::vector<SortEntry> &entries) -> MergeSortRun {
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  std::vector<page_id_t> pages;
  page_id_t page_id = INVALID_PAGE_ID;

  for (const auto &entry : entries) {
    bool inserted = false;
    while (!inserted) {
      if (page_id == INVALID_PAGE_ID) {
        page_id = bpm->NewPage();
        auto guard = bpm->WritePage(page_id);
        guard.AsMut<IntermediateResultPage>()->Init();
        pages.emplace_back(page_id);
      }
      auto guard = bpm->WritePage(page_id);
      auto *page = guard.AsMut<IntermediateResultPage>();
      inserted = page->InsertTuple(entry.second);
      if (!inserted) {
        // Current page is full: start a new page and link it.
        auto next_page_id = bpm->NewPage();
        auto next_guard = bpm->WritePage(next_page_id);
        next_guard.AsMut<IntermediateResultPage>()->Init();
        page->SetNextPageId(next_page_id);
        page_id = next_page_id;
      }
    }
  }

  return MergeSortRun(std::move(pages), bpm);
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeTwoRuns(MergeSortRun &left, MergeSortRun &right) -> MergeSortRun {
  auto *bpm = exec_ctx_->GetBufferPoolManager();

  std::vector<page_id_t> pages;
  page_id_t page_id = INVALID_PAGE_ID;


  auto append_tuple = [&](const Tuple &tuple) {
    bool inserted = false;
    while (!inserted) {
      if (page_id == INVALID_PAGE_ID) {
        page_id = bpm->NewPage();
        auto guard = bpm->WritePage(page_id);
        guard.AsMut<IntermediateResultPage>()->Init();
        pages.emplace_back(page_id);
      }
      auto guard = bpm->WritePage(page_id);
      auto *page = guard.AsMut<IntermediateResultPage>();
      inserted = page->InsertTuple(tuple);
      if (!inserted) {
        auto next_page_id = bpm->NewPage();
        auto next_guard = bpm->WritePage(next_page_id);
        next_guard.AsMut<IntermediateResultPage>()->Init();
        page->SetNextPageId(next_page_id);
        page_id = next_page_id;
      }
    }

  };

  auto left_iter = left.Begin();
  auto left_end = left.End();
  auto right_iter = right.Begin();
  auto right_end = right.End();
  const auto &schema = child_executor_->GetOutputSchema();
  const auto &order_bys = plan_->GetOrderBy();
  auto cmp = cmp_;

  while (left_iter != left_end || right_iter != right_end) {
    if (left_iter == left_end) {
      append_tuple(*right_iter);
      ++right_iter;
      continue;
    }
    if (right_iter == right_end) {
      append_tuple(*left_iter);
      ++left_iter;
      continue;
    }
    auto left_tuple = *left_iter;
    auto right_tuple = *right_iter;
    auto left_key = GenerateSortKey(left_tuple, order_bys, schema);
    auto right_key = GenerateSortKey(right_tuple, order_bys, schema);
    if (cmp(SortEntry{right_key, right_tuple}, SortEntry{left_key, left_tuple})) {
      append_tuple(right_tuple);
      ++right_iter;
    } else {
      append_tuple(left_tuple);
      ++left_iter;
    }
  }

  return MergeSortRun(std::move(pages), bpm);
}

/**
 * Yield the next tuple batch from the external merge sort.
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (!result_run_.has_value() || !result_iter_.has_value()) {
    return false;
  }

  auto end = result_run_->End();
  while (*result_iter_ != end && tuple_batch->size() < batch_size) {
    tuple_batch->emplace_back(**result_iter_);
    rid_batch->emplace_back(RID{});
    ++(*result_iter_);
  }

  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
