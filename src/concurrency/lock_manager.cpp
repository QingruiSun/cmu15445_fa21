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
#include "concurrency/transaction_manager.h"
#include "common/logger.h"

#include <utility>
#include <vector>
#include <iterator>
#include <list>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
	std::unique_lock lock(latch_);
	if (txn->GetState() == TransactionState::ABORTED) {
		return false;
	}
	if (txn->GetState() == TransactionState::SHRINKING) {
		txn->SetState(TransactionState::ABORTED);
		return false;
	}
	if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
		return true;
	}

	LOG_DEBUG("shared lock start");

	lock_table_[rid].request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
	bool granted = false;
	while (!granted) {
		granted = true;
		if (!lock_table_[rid].request_queue_.empty()) {
			for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
				if ((lock_request.txn_id_ != txn->GetTransactionId()) && (lock_request.granted_) && (lock_request.lock_mode_ != LockMode::SHARED)) {
					Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
					hold_txn->SetState(TransactionState::ABORTED);
					granted = false;
					lock_table_[rid].cv_.wait(lock);
					break;
				}
			}
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
	LOG_DEBUG("shared lock succeed");
	return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
	std::unique_lock lock(latch_);
	if (txn->GetState() == TransactionState::ABORTED) {
		return false;
	}
	if (txn->GetState() == TransactionState::SHRINKING) {
		txn->SetState(TransactionState::ABORTED);
		return false;
	}
	for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
		if (lock_request.txn_id_ == txn->GetTransactionId()) {
			return false;
		}
	}

	lock_table_[rid].request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
	bool granted = false;
	while (!granted) {
		granted = true;
		if (!lock_table_[rid].request_queue_.empty()) {
			for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
			        if ((lock_request.txn_id_ != txn->GetTransactionId()) && (lock_request.granted_)) {
					Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
					hold_txn->SetState(TransactionState::ABORTED);
				        granted = false;
			        }
			}
			if (!granted) {
				lock_table_[rid].cv_.wait(lock);
			}
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
	if (txn->GetState() == TransactionState::ABORTED) {
		return false;
	}
	if (txn->GetState() == TransactionState::SHRINKING) {
		txn->SetState(TransactionState::ABORTED);
		return false;
	}
	for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
		if ((lock_request.txn_id_ == txn->GetTransactionId()) && (lock_request.lock_mode_ == LockMode::EXCLUSIVE)) {
			return false;
		}
	}
	if (!txn->IsSharedLocked(rid)) {
		return false;
	}

	bool granted = false;
	while (!granted) {
		granted = true;
		if (!lock_table_[rid].request_queue_.empty()) {
			for (const LockRequest &lock_request : lock_table_[rid].request_queue_) {
			        if ((lock_request.txn_id_ != txn->GetTransactionId()) && (lock_request.granted_)) {
					Transaction *hold_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
					hold_txn->SetState(TransactionState::ABORTED);
				        granted = false;
			        }
			}
			if (!granted) {
				lock_table_[rid].cv_.wait(lock);
			}
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
	if (txn->GetState() == TransactionState::GROWING) {
		txn->SetState(TransactionState::SHRINKING);
	}

	LOG_DEBUG("unlock");

	std::list<LockRequest>::iterator lock_request_iter = lock_table_[rid].request_queue_.begin();
	while (lock_request_iter != lock_table_[rid].request_queue_.end()) {
		if ((lock_request_iter->txn_id_ == txn->GetTransactionId()) && (lock_request_iter->granted_)) {
			if (lock_request_iter->lock_mode_ == LockMode::SHARED) {
				txn->GetSharedLockSet()->erase(rid);
			} else {
				txn->GetExclusiveLockSet()->erase(rid);
			}
			if (txn->GetState() == TransactionState::GROWING) {
				txn->SetState(TransactionState::SHRINKING);
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
