//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  predicate_ = plan_->Predicate();
  left_remain_ = left_executor_->Next(&left_tuple_, &left_rid_);
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      right_executor_->Init();
      left_remain_ = left_executor_->Next(&left_tuple_, &left_rid_);
      if (!left_remain_) {
        return false;
      }
      right_executor_->Next(&right_tuple, &right_rid);
    }
    if (predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
      std::vector<Value> vals;
      for (const Column &column : plan_->OutputSchema()->GetColumns()) {
        vals.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_));
      }
      *tuple = Tuple(vals, plan_->OutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
