//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <iterator>
#include <list>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::ImplicityAbort(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    ImplicityAbort(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if ((txn->GetState() == TransactionState::SHRINKING) &&
      (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
    ImplicityAbort(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  bool granted = false;
  while (!granted) {
    granted = true;
    if (!lock_table_[rid].request_queue_.empty()) {
      bool is_abort_another = false;
      std::vector<LockRequest> delete_vector;
      for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
        if ((txn->GetTransactionId() < lock_request.txn_id_) && (lock_request.lock_mode_ != LockMode::SHARED)) {
          Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
          hold_txn->SetState(TransactionState::ABORTED);
          delete_vector.push_back(lock_request);
          is_abort_another = true;
        }
      }
      for (const LockRequest &lock_request : delete_vector) {
        lock_table_[rid].request_queue_.remove(lock_request);
      }
      if (is_abort_another) {
        lock_table_[rid].cv_.notify_all();
      }
      for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
        if ((txn->GetTransactionId() > lock_request.txn_id_) && (lock_request.granted_) &&
            (lock_request.lock_mode_ != LockMode::SHARED)) {
          granted = false;
          lock_table_[rid].cv_.wait(lock);
          break;
        }
      }
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      ImplicityAbort(txn, AbortReason::DEADLOCK);
    }
  }

  std::list<LockRequest>::iterator lock_request_iter = lock_table_[rid].request_queue_.begin();
  while (lock_request_iter != lock_table_[rid].request_queue_.end()) {
    if (lock_request_iter->txn_id_ == txn->GetTransactionId()) {
      lock_request_iter->granted_ = true;
    }
    ++lock_request_iter;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock lock(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    ImplicityAbort(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  bool granted = false;
  while (!granted) {
    granted = true;
    if (!lock_table_[rid].request_queue_.empty()) {
      for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
        if ((txn->GetTransactionId() < lock_request.txn_id_)) {
          Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
          hold_txn->SetState(TransactionState::ABORTED);
        }
        if ((txn->GetTransactionId() > lock_request.txn_id_) && (lock_request.granted_)) {
          granted = false;
        }
      }
      if (!granted) {
        lock_table_[rid].cv_.wait(lock);
      }
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      ImplicityAbort(txn, AbortReason::DEADLOCK);
    }
  }

  std::list<LockRequest>::iterator lock_request_iter = lock_table_[rid].request_queue_.begin();
  while (lock_request_iter != lock_table_[rid].request_queue_.end()) {
    if (lock_request_iter->txn_id_ == txn->GetTransactionId()) {
      lock_request_iter->granted_ = true;
    }
    ++lock_request_iter++;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock lock(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    ImplicityAbort(txn, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  bool granted = false;
  while (!granted) {
    granted = true;
    if (!lock_table_[rid].request_queue_.empty()) {
      for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
        if ((txn->GetTransactionId() < lock_request.txn_id_)) {
          Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
          hold_txn->SetState(TransactionState::ABORTED);
        }
        if ((txn->GetTransactionId() > lock_request.txn_id_) && (lock_request.granted_)) {
          granted = false;
        }
      }
      if (!granted) {
        lock_table_[rid].cv_.wait(lock);
      }
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      ImplicityAbort(txn, AbortReason::DEADLOCK);
    }
  }

  std::list<LockRequest>::iterator lock_request_iter = lock_table_[rid].request_queue_.begin();
  while (lock_request_iter != lock_table_[rid].request_queue_.end()) {
    if (lock_request_iter->txn_id_ == txn->GetTransactionId()) {
      lock_request_iter->granted_ = true;
      lock_request_iter->lock_mode_ = LockMode::EXCLUSIVE;
    }
    ++lock_request_iter;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock lock(latch_);
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) && (txn->GetState() == TransactionState::GROWING)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::list<LockRequest>::iterator lock_request_iter = lock_table_[rid].request_queue_.begin();
  while (lock_request_iter != lock_table_[rid].request_queue_.end()) {
    if ((lock_request_iter->txn_id_ == txn->GetTransactionId()) && (lock_request_iter->granted_)) {
      if (lock_request_iter->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
      }
      lock_table_[rid].request_queue_.erase(lock_request_iter);
      break;
    }
    ++lock_request_iter;
  }
  lock_table_[rid].cv_.notify_all();
  return true;
}

}  // namespace bustub
