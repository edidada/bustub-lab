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
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

namespace {

/** Collect all column value expressions referenced by `expr`. */
void CollectColumns(const AbstractExpressionRef &expr, std::vector<const ColumnValueExpression *> *columns) {
  if (const auto *column = dynamic_cast<const ColumnValueExpression *>(expr.get()); column != nullptr) {
    columns->emplace_back(column);
    return;
  }
  for (const auto &child : expr->GetChildren()) {
    CollectColumns(child, columns);
  }
}

}  // namespace

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

  auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
  if (table_info == nullptr) {
    return optimized_plan;
  }
  const auto table_indexes = catalog_.GetTableIndexes(table_info->name_);

  // Fast path: `indexed_column = constant` becomes a point lookup.
  const auto *comparison = dynamic_cast<const ComparisonExpression *>(seq_scan.filter_predicate_.get());
  if (comparison != nullptr && comparison->comp_type_ == ComparisonType::Equal) {
    const auto *left_column = dynamic_cast<const ColumnValueExpression *>(comparison->children_[0].get());
    const auto *right_column = dynamic_cast<const ColumnValueExpression *>(comparison->children_[1].get());

    uint32_t column_idx = 0;
    AbstractExpressionRef key_expr;
    bool is_point_lookup = false;
    if (left_column != nullptr && right_column == nullptr) {
      column_idx = left_column->GetColIdx();
      key_expr = comparison->children_[1];
      is_point_lookup = true;
    } else if (left_column == nullptr && right_column != nullptr) {
      column_idx = right_column->GetColIdx();
      key_expr = comparison->children_[0];
      is_point_lookup = true;
    }

    if (is_point_lookup) {
      for (const auto &index_info : table_indexes) {
        const auto &key_attrs = index_info->index_->GetKeyAttrs();
        if (key_attrs.size() == 1 && key_attrs[0] == column_idx) {
          return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                     index_info->index_oid_, seq_scan.filter_predicate_,
                                                     std::vector<AbstractExpressionRef>{key_expr});
        }
      }
    }
  }

  // General path: any other predicate over an indexed column becomes a full index scan with the stored filter.
  std::vector<const ColumnValueExpression *> referenced_columns;
  CollectColumns(seq_scan.filter_predicate_, &referenced_columns);
  for (const auto *column : referenced_columns) {
    if (column->GetTupleIdx() != 0) {
      continue;
    }
    for (const auto &index_info : table_indexes) {
      const auto &key_attrs = index_info->index_->GetKeyAttrs();
      if (key_attrs.size() == 1 && key_attrs[0] == column->GetColIdx()) {
        return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                   index_info->index_oid_, seq_scan.filter_predicate_,
                                                   std::vector<AbstractExpressionRef>{});
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
