//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      having_(plan_->GetHaving()),
      aht_iterator_(aht_.Begin()),
      aht_end_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple child_tuple;
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    AggregateKey agg_key = MakeAggregateKey(&child_tuple);
    AggregateValue agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
  aht_end_ = aht_.End();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_end_) {
    AggregateKey agg_key = aht_iterator_.Key();
    AggregateValue agg_val = aht_iterator_.Val();
    bool output = false;
    if (having_ != nullptr) {
      if (having_->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
        output = true;
      }
    } else {
      output = true;
    }
    if (output) {
      std::vector<Value> vals;
      for (const Column &column : GetOutputSchema()->GetColumns()) {
        Value val = column.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_);
        vals.emplace_back(val);
      }
      *tuple = Tuple(vals, GetOutputSchema());
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
