//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    Tuple right_tuple;
    Value left_key = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_->GetOutputSchema());
    hash_t hash_key = HashUtil::HashValue(&left_key);
    ht_[hash_key].emplace_back(left_tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  Tuple left_tuple;
  if (!results_.empty()) {
    *tuple = results_.front();
    results_.pop_front();
    return true;
  }
  while (right_child_->Next(&right_tuple, &right_rid)) {
    Value right_key = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
    hash_t hash_key = HashUtil::HashValue(&right_key);
    if (ht_.find(hash_key) != ht_.end()) {
      for (Tuple &tuple : ht_[hash_key]) {
        Value left_key = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
        if (left_key.CompareEquals(right_key) == CmpBool::CmpTrue) {
          std::vector<Value> vals;
          for (const Column &column : GetOutputSchema()->GetColumns()) {
            Value val = column.GetExpr()->EvaluateJoin(&tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                       right_child_->GetOutputSchema());
            vals.emplace_back(val);
          }
          Tuple result_tuple = Tuple(vals, GetOutputSchema());
          results_.push_back(result_tuple);
        }
      }
      if (!results_.empty()) {
        break;
      }
    }
  }
  if (!results_.empty()) {
    *tuple = results_.front();
    results_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
