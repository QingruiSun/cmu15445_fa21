//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <utility>

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

hash_t DistinctExecutor::GetHashValue(const Tuple &tuple) {
  hash_t hash_value = 0;
  bool first_hash = true;
  for (const Column &column : child_executor_->GetOutputSchema()->GetColumns()) {
    Value val = tuple.GetValue(plan_->GetChildPlan()->OutputSchema(),
                               plan_->GetChildPlan()->OutputSchema()->GetColIdx(column.GetName()));
    if (first_hash) {
      hash_value = HashUtil::HashValue(&val);
      first_hash = false;
    } else {
      hash_t next_hash_value = HashUtil::HashValue(&val);
      hash_value = HashUtil::CombineHashes(hash_value, next_hash_value);
    }
  }
  return hash_value;
}

void DistinctExecutor::Init() {
  Tuple child_tuple;
  RID child_rid;
  child_executor_->Init();
  while (true) {
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
      break;
    }
    bool exist_same = false;
    hash_t hash_val = DistinctExecutor::GetHashValue(child_tuple);
    if (ht_[hash_val].empty()) {
      ht_[hash_val].emplace_back(child_tuple);
    } else {
      for (Tuple &tuple : ht_[hash_val]) {
        bool is_same = true;
        for (const Column &column : child_executor_->GetOutputSchema()->GetColumns()) {
          Value val = tuple.GetValue(plan_->GetChildPlan()->OutputSchema(),
                                     plan_->GetChildPlan()->OutputSchema()->GetColIdx(column.GetName()));
          Value child_val = child_tuple.GetValue(plan_->GetChildPlan()->OutputSchema(),
                                                 plan_->GetChildPlan()->OutputSchema()->GetColIdx(column.GetName()));
          if (val.CompareNotEquals(child_val) == CmpBool::CmpTrue) {
            is_same = false;
            break;
          }
        }
        if (is_same) {
          exist_same = true;
          break;
        }
      }
      if (!exist_same) {
        ht_[hash_val].emplace_back(child_tuple);
      }
    }
  }
  ht_iterator_ = ht_.begin();
  ht_end_ = ht_.end();
  if (ht_iterator_ != ht_end_) {  // hash table is not empty
    tmp_iterator_ = ht_iterator_->second.begin();
    tmp_end_ = ht_iterator_->second.end();
  }
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (ht_iterator_ == ht_end_) {
    return false;
  }
  if (tmp_iterator_ == tmp_end_) {
    ++ht_iterator_;
    if (ht_iterator_ == ht_end_) {
      return false;
    }
    tmp_iterator_ = ht_iterator_->second.begin();
    tmp_end_ = ht_iterator_->second.end();
  }
  *tuple = *tmp_iterator_;
  ++tmp_iterator_;
  return true;
}

}  // namespace bustub
