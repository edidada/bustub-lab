//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

namespace {

/**
 * Flattens a conjunction of equi-conditions into pairs of (left key, right key) expressions,
 * where both key expressions are normalized to be evaluated on their own single input tuple.
 * @return `true` if every conjunct is an equality between a left column and a right column
 */
auto CollectEquiConditions(const AbstractExpressionRef &expr,
                           std::vector<std::pair<AbstractExpressionRef, AbstractExpressionRef>> *keys) -> bool {
  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr.get()); logic != nullptr) {
    if (logic->logic_type_ != LogicType::And) {
      return false;
    }
    return CollectEquiConditions(logic->GetChildAt(0), keys) && CollectEquiConditions(logic->GetChildAt(1), keys);
  }

  const auto *comparison = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comparison == nullptr || comparison->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  const auto *left = dynamic_cast<const ColumnValueExpression *>(comparison->children_[0].get());
  const auto *right = dynamic_cast<const ColumnValueExpression *>(comparison->children_[1].get());
  if (left == nullptr || right == nullptr) {
    return false;
  }

  auto make_key = [](const ColumnValueExpression *column) {
    return std::make_shared<ColumnValueExpression>(0, column->GetColIdx(), column->GetReturnType());
  };

  if (left->GetTupleIdx() == 0 && right->GetTupleIdx() == 1) {
    keys->emplace_back(make_key(left), make_key(right));
    return true;
  }
  if (left->GetTupleIdx() == 1 && right->GetTupleIdx() == 0) {
    keys->emplace_back(make_key(right), make_key(left));
    return true;
  }
  return false;
}

}  // namespace

/**
 * @brief optimize nested loop join into hash join.
 * We support join keys of any number of conjunction of equi-conditions:
 * E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

  std::vector<std::pair<AbstractExpressionRef, AbstractExpressionRef>> keys;
  if (!CollectEquiConditions(nlj_plan.Predicate(), &keys) || keys.empty()) {
    return optimized_plan;
  }

  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  left_keys.reserve(keys.size());
  right_keys.reserve(keys.size());
  for (auto &[left, right] : keys) {
    left_keys.emplace_back(std::move(left));
    right_keys.emplace_back(std::move(right));
  }

  return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                            std::move(left_keys), std::move(right_keys), nlj_plan.GetJoinType());
}

}  // namespace bustub
