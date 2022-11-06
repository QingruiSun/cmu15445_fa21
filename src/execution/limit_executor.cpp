//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void LimitExecutor::Init() {
  child_executor_->Init();
  limit_ = plan_->GetLimit();
  num_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (num_ >= limit_) {
    return false;
  }
  Tuple src_tuple;
  RID src_rid;
  if (!child_executor_->Next(&src_tuple, &src_rid)) {
    return false;
  }
  *tuple = src_tuple;
  *rid = src_rid;
  ++num_;
  return true;
}

}  // namespace bustub
