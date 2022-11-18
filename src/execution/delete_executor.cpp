//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  lock_mgr_ = exec_ctx_->GetLockManager();
  txn_ = exec_ctx_->GetTransaction();
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_oid_t table_oid = plan_->TableOid();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(table_oid);
  indexes_ = catalog_->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple delete_tuple;
  RID delete_rid;
  if (!child_executor_->Next(&delete_tuple, &delete_rid)) {
    return false;
  }
  if (txn_->IsSharedLocked(delete_rid)) {
    if (!lock_mgr_->LockUpgrade(txn_, delete_rid)) {
      return false;
    }
  } else {
    if (!lock_mgr_->LockExclusive(txn_, delete_rid)) {
      return false;
    }
  }
  table_info_->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());
  for (IndexInfo *index_info : indexes_) {
    std::vector<uint32_t> key_attrs = index_info->index_->GetKeyAttrs();
    Tuple key_tuple = delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs);
    index_info->index_->DeleteEntry(key_tuple, delete_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord wr(*rid, table_info_->oid_, WType::DELETE, delete_tuple, index_info->index_oid_,
                        exec_ctx_->GetCatalog());
    txn_->GetIndexWriteSet()->push_back(wr);
  }
  return true;
}

}  // namespace bustub
