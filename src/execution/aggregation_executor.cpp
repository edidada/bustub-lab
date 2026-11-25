//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  size_t input_rows = 0;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
      input_rows++;
    }
  }

  default_row_.reset();

  // For an aggregation without group by over an empty input, a single default row is produced:
  // COUNT yields 0, while SUM/MIN/MAX yield NULL.
  if (input_rows == 0 && plan_->GetGroupBys().empty()) {
    std::vector<Value> values;
    const auto &agg_types = plan_->GetAggregateTypes();
    const auto &output_schema = GetOutputSchema();
    for (uint32_t i = 0; i < agg_types.size(); i++) {
      if (agg_types[i] == AggregationType::CountStarAggregate) {
        values.emplace_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.emplace_back(ValueFactory::GetNullValueByType(output_schema.GetColumn(i).GetType()));
      }
    }
    default_row_ = std::move(values);
  }

  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_.Begin());
}

/**
 * Yield the next tuple batch from the aggregation.
 */
auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (default_row_.has_value() && tuple_batch->empty()) {
    tuple_batch->emplace_back(Tuple(*default_row_, &GetOutputSchema()));
    rid_batch->emplace_back(RID{});
    default_row_.reset();
    return true;
  }

  while (tuple_batch->size() < batch_size && *aht_iterator_ != aht_.End()) {
    std::vector<Value> values;
    values.reserve(aht_iterator_->Key().group_bys_.size() + aht_iterator_->Val().aggregates_.size());
    for (const auto &value : aht_iterator_->Key().group_bys_) {
      values.emplace_back(value);
    }
    for (const auto &value : aht_iterator_->Val().aggregates_) {
      values.emplace_back(value);
    }
    tuple_batch->emplace_back(Tuple(std::move(values), &GetOutputSchema()));
    rid_batch->emplace_back(RID{});
    ++(*aht_iterator_);
  }

  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
