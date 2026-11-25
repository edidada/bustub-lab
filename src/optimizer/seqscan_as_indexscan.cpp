//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"

#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  if (seq_scan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  // Only equality predicates on a single-column index can be turned into a point lookup.
  const auto *comparison = dynamic_cast<const ComparisonExpression *>(seq_scan.filter_predicate_.get());
  if (comparison == nullptr || comparison->comp_type_ != ComparisonType::Equal) {
    return optimized_plan;
  }

  const auto *left_column = dynamic_cast<const ColumnValueExpression *>(comparison->children_[0].get());
  const auto *right_column = dynamic_cast<const ColumnValueExpression *>(comparison->children_[1].get());

  uint32_t column_idx = 0;
  AbstractExpressionRef key_expr;
  if (left_column != nullptr && right_column == nullptr) {
    column_idx = left_column->GetColIdx();
    key_expr = comparison->children_[1];
  } else if (left_column == nullptr && right_column != nullptr) {
    column_idx = right_column->GetColIdx();
    key_expr = comparison->children_[0];
  } else {
    return optimized_plan;
  }

  auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
  if (table_info == nullptr) {
    return optimized_plan;
  }

  for (const auto &index_info : catalog_.GetTableIndexes(table_info->name_)) {
    const auto &key_attrs = index_info->index_->GetKeyAttrs();
    if (key_attrs.size() == 1 && key_attrs[0] == column_idx) {
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                 index_info->index_oid_, seq_scan.filter_predicate_,
                                                 std::vector<AbstractExpressionRef>{key_expr});
    }
  }

  return optimized_plan;
}

}  // namespace bustub
