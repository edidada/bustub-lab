//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** A join key, i.e. a list of values from the join key expressions. */
struct HashJoinKey {
  std::vector<Value> values_;

  auto operator==(const HashJoinKey &other) const -> bool {
    if (values_.size() != other.values_.size()) {
      return false;
    }
    for (size_t i = 0; i < values_.size(); i++) {
      if (values_[i].IsNull() || other.values_[i].IsNull()) {
        return false;
      }
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** Hasher for HashJoinKey. */
struct HashJoinKeyHasher {
  auto operator()(const HashJoinKey &key) const -> size_t {
    size_t hash = 0;
    for (const auto &value : key.values_) {
      if (!value.IsNull()) {
        hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&value));
      }
    }
    return hash;
  }
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** The right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** The build side hash table: join key of a right tuple to right tuples */
  std::unordered_map<HashJoinKey, std::vector<Tuple>, HashJoinKeyHasher> hash_table_;

  /** The left tuples to be probed */
  std::vector<Tuple> left_tuples_;

  /** The position of the next left tuple to be probed */
  size_t left_pos_{0};

  /** The current left tuple being probed */
  std::optional<Tuple> left_tuple_;

  /** The matching right tuples of the current left tuple */
  std::vector<Tuple> matches_;

  /** The position of the next matching right tuple to be emitted */
  size_t match_pos_{0};
};

}  // namespace bustub
