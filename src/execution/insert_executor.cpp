//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
  catalog_ = exec_ctx_->GetCatalog();
  child_executor_ = std::move(child_executor);
  lock_mgr_ = exec_ctx_->GetLockManager();
  txn_ = exec_ctx_->GetTransaction();
}

void InsertExecutor::Init() {
  table_oid_t table_oid = plan_->TableOid();
  table_info_ = catalog_->GetTable(table_oid);
  indexes_ = catalog_->GetTableIndexes(table_info_->name_);
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  } else {
    raw_value_size_ = plan_->RawValues().size();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple insert_tuple;
  RID insert_rid;
  if (plan_->IsRawInsert()) {
    if (raw_insert_index_ >= raw_value_size_) {
      return false;
    }
    insert_tuple = Tuple(plan_->RawValuesAt(raw_insert_index_), &table_info_->schema_);
    ++raw_insert_index_;
  } else {
    if (!child_executor_->Next(&insert_tuple, &insert_rid)) {
      return false;
    }
  }
  assert(table_info_->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction()));
  if (!lock_mgr_->LockExclusive(txn_, insert_rid)) {
    return false;
  }
  for (IndexInfo *index_info : indexes_) {
    std::vector<uint32_t> key_attrs = index_info->index_->GetKeyAttrs();
    Tuple key_tuple = insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs);
    index_info->index_->InsertEntry(key_tuple, insert_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord wr(insert_rid, table_info_->oid_, WType::INSERT, insert_tuple, index_info->index_oid_,
                        exec_ctx_->GetCatalog());
    txn_->GetIndexWriteSet()->push_back(wr);
  }
  return true;
}
}  // namespace bustub
