//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <string>
#include <utility>
#include <vector>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  indexes_ = catalog_->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple src_tuple;
  RID src_rid;
  if (!child_executor_->Next(&src_tuple, &src_rid)) {
    return false;
  }
  RID update_rid = src_rid;
  Tuple update_tuple = GenerateUpdatedTuple(src_tuple);
  if (txn_->IsSharedLocked(update_rid)) {
    if (!lock_mgr_->LockUpgrade(txn_, update_rid)) {
      return false;
    }
  } else {
    if (!lock_mgr_->LockExclusive(txn_, update_rid)) {
      return false;
    }
  }
  bool is_update_succeed = table_info_->table_->UpdateTuple(update_tuple, src_rid, exec_ctx_->GetTransaction());
  if (!is_update_succeed) {
    table_info_->table_->MarkDelete(src_rid, exec_ctx_->GetTransaction());
    table_info_->table_->InsertTuple(update_tuple, &update_rid, exec_ctx_->GetTransaction());
  }
  for (IndexInfo *index_info : indexes_) {
    std::vector<uint32_t> key_attrs = index_info->index_->GetKeyAttrs();
    Tuple old_key_tuple = src_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs);
    index_info->index_->DeleteEntry(old_key_tuple, src_rid, exec_ctx_->GetTransaction());
    Tuple update_key_tuple = update_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs);
    index_info->index_->InsertEntry(update_key_tuple, update_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord wr(*rid, table_info_->oid_, WType::UPDATE, update_tuple, src_tuple, index_info->index_oid_,
                        exec_ctx_->GetCatalog());
    txn_->AppendIndexWriteRecord(wr);
  }
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
