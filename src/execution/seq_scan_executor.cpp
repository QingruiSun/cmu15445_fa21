//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_oid_(plan_->GetTableOid()),
      table_info_(exec_ctx_->GetCatalog()->GetTable(table_oid_)),
      table_heap_(table_info_->table_.get()),
      table_iterator_(table_heap_->Begin(exec_ctx_->GetTransaction())),
      table_iterator_end_(table_heap_->End()) {}

void SeqScanExecutor::Init() {
  table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
  table_iterator_end_ = table_heap_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iterator_ != table_iterator_end_) {
    if (plan_->GetPredicate() == nullptr) {
      break;
    }
    if (!plan_->GetPredicate()->Evaluate(&(*table_iterator_), &table_info_->schema_).GetAs<bool>()) {
      ++table_iterator_;
    } else {
      break;
    }
  }
  if (table_iterator_ == table_iterator_end_) {
    return false;
  }
  std::vector<Value> vals;
  for (const auto &column : plan_->OutputSchema()->GetColumns()) {
    vals.emplace_back(column.GetExpr()->Evaluate(&(*table_iterator_), &table_info_->schema_));
  }
  *tuple = Tuple(vals, plan_->OutputSchema());
  *rid = (*table_iterator_).GetRid();
  ++table_iterator_;
  return true;
}

}  // namespace bustub
