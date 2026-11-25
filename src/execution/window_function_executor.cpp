//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/execution_common.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

/** Computes the aggregate of the given rows in the frame. */
auto AggregateFrame(WindowFunctionType type, const AbstractExpressionRef &function,
                    const std::vector<Tuple> &tuples, const std::vector<size_t> &frame, const Schema &schema) -> Value {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      return ValueFactory::GetIntegerValue(static_cast<int32_t>(frame.size()));
    case WindowFunctionType::CountAggregate: {
      int32_t count = 0;
      for (const auto idx : frame) {
        auto value = function->Evaluate(&tuples[idx], schema);
        if (!value.IsNull()) {
          count++;
        }
      }
      return ValueFactory::GetIntegerValue(count);
    }
    case WindowFunctionType::SumAggregate: {
      Value result = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      for (const auto idx : frame) {
        auto value = function->Evaluate(&tuples[idx], schema);
        if (value.IsNull()) {
          continue;
        }
        result = result.IsNull() ? value : result.Add(value);
      }
      return result;
    }
    case WindowFunctionType::MinAggregate: {
      Value result = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      for (const auto idx : frame) {
        auto value = function->Evaluate(&tuples[idx], schema);
        if (value.IsNull()) {
          continue;
        }
        result = result.IsNull() ? value : result.Min(value);
      }
      return result;
    }
    case WindowFunctionType::MaxAggregate: {
      Value result = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      for (const auto idx : frame) {
        auto value = function->Evaluate(&tuples[idx], schema);
        if (value.IsNull()) {
          continue;
        }
        result = result.IsNull() ? value : result.Max(value);
      }
      return result;
    }
    case WindowFunctionType::Rank: {
      Value result = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      return result;
    }
  }
  return ValueFactory::GetNullValueByType(TypeId::INTEGER);
}

}  // namespace

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  results_.clear();
  result_pos_ = 0;

  const auto &child_schema = child_executor_->GetOutputSchema();

  std::vector<Tuple> tuples;
  std::vector<RID> rids;

  std::vector<Tuple> input;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      input.emplace_back(tuple);
    }
  }

  // Computed window values: [row index][window function column index]
  std::unordered_map<size_t, std::unordered_map<uint32_t, Value>> computed;

  for (const auto &[column_idx, function] : plan_->window_functions_) {
    // Partition the input rows.
    std::unordered_map<AggregateKey, std::vector<size_t>> partitions;
    for (size_t row = 0; row < input.size(); row++) {
      AggregateKey key;
      for (const auto &expr : function.partition_by_) {
        key.group_bys_.emplace_back(expr->Evaluate(&input[row], child_schema));
      }
      partitions[key].emplace_back(row);
    }

    TupleComparator comparator(function.order_by_);
    const bool has_order_by = !function.order_by_.empty();

    for (auto &[key, rows] : partitions) {
      // Keep the rows in partition order.
      std::vector<size_t> ordered = rows;
      std::vector<SortKey> sort_keys;
      sort_keys.reserve(ordered.size());
      for (const auto row : ordered) {
        sort_keys.emplace_back(GenerateSortKey(input[row], function.order_by_, child_schema));
      }

      auto key_less = [&](size_t a, size_t b) {
        return comparator(SortEntry{sort_keys[a], input[a]}, SortEntry{sort_keys[b], input[b]});
      };

      if (has_order_by) {
        std::stable_sort(ordered.begin(), ordered.end(), key_less);
      }

      for (size_t pos = 0; pos < ordered.size(); pos++) {
        const auto row = ordered[pos];

        if (function.type_ == WindowFunctionType::Rank) {
          int32_t rank = static_cast<int32_t>(pos) + 1;
          if (has_order_by) {
            // Peers share the smallest rank of the group.
            size_t first_peer = pos;
            while (first_peer > 0 &&
                   !comparator(SortEntry{sort_keys[ordered[first_peer - 1]], input[ordered[first_peer - 1]]},
                               SortEntry{sort_keys[row], input[row]}) &&
                   !comparator(SortEntry{sort_keys[row], input[row]},
                               SortEntry{sort_keys[ordered[first_peer - 1]], input[ordered[first_peer - 1]]})) {
              first_peer--;
            }
            rank = static_cast<int32_t>(first_peer) + 1;
          } else {
            rank = 1;
          }
          computed[row][column_idx] = ValueFactory::GetIntegerValue(rank);
          continue;
        }

        std::vector<size_t> frame;
        if (!has_order_by) {
          frame = ordered;
        } else {
          for (size_t other = 0; other <= pos; other++) {
            frame.emplace_back(ordered[other]);
          }
          // Include peers of the current row (RANGE semantics).
          for (size_t other = pos + 1; other < ordered.size(); other++) {
            if (comparator(SortEntry{sort_keys[ordered[other]], input[ordered[other]]},
                           SortEntry{sort_keys[row], input[row]}) ||
                comparator(SortEntry{sort_keys[row], input[row]},
                           SortEntry{sort_keys[ordered[other]], input[ordered[other]]})) {
              break;
            }
            frame.emplace_back(ordered[other]);
          }
        }

        computed[row][column_idx] =
            AggregateFrame(function.type_, function.function_, input, frame, child_schema);
      }
    }
  }

  // Build the output tuples, preserving the input row order.
  results_.clear();
  results_.reserve(input.size());
  for (size_t row = 0; row < input.size(); row++) {
    std::vector<Value> values;
    values.reserve(plan_->columns_.size());
    for (size_t i = 0; i < plan_->columns_.size(); i++) {
      if (auto iter = computed[row].find(static_cast<uint32_t>(i)); iter != computed[row].end()) {
        values.emplace_back(iter->second);
        continue;
      }
      if (plan_->window_functions_.count(static_cast<uint32_t>(i)) > 0) {
        values.emplace_back(ValueFactory::GetNullValueByType(plan_->OutputSchema().GetColumn(i).GetType()));
        continue;
      }
      values.emplace_back(plan_->columns_[i]->Evaluate(&input[row], child_schema));
    }
    results_.emplace_back(Tuple(std::move(values), &GetOutputSchema()));
  }
}

/**
 * Yield the next tuple batch from the window aggregation.
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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
