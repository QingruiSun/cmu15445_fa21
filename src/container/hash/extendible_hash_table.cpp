//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

/**
 * First the extendible hash table looks like this:
 * global depth = 0
 * local depth = 0
 * ------             -----
 *|  0   | --------> |     |
 * ------            |     |bucket
 *                   |     |
 *                    -----
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  page_id_t first_bucket_page_id;
  assert(buffer_pool_manager_->NewPage(&first_bucket_page_id) != nullptr);
  dir_page->Init();
  dir_page->SetPageId(directory_page_id_);
  dir_page->SetLocalDepth(0, 0);
  dir_page->SetBucketPageId(0, first_bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(first_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return hash_fn_.GetHash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *dir_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *raw_bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  raw_bucket_page->RLatch();
  auto *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_bucket_page->GetData());
  bool matched = bucket_page->GetValue(key, comparator_, result);
  raw_bucket_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return matched;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *raw_bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  raw_bucket_page->WLatch();
  auto *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_bucket_page->GetData());
  bool is_succeed = false;
  if (!bucket_page->IsFull()) {
    is_succeed = bucket_page->Insert(key, value, comparator_);
    raw_bucket_page->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    return is_succeed;
  }
  std::vector<ValueType> res;
  bucket_page->GetValue(key, comparator_, &res);
  if (!res.empty()) {
    for (const auto &val : res) {
      if (val == value) {
        raw_bucket_page->WUnlatch();
        table_latch_.RUnlock();
        buffer_pool_manager_->UnpinPage(directory_page_id_, false);
        buffer_pool_manager_->UnpinPage(bucket_page_id, false);
        return false;
      }
    }
  }
  raw_bucket_page->WUnlatch();
  table_latch_.RUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  is_succeed = SplitInsert(transaction, key, value);
  return is_succeed;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t old_bucket_page_id = KeyToPageId(key, dir_page);
  auto *raw_old_bucket_page = buffer_pool_manager_->FetchPage(old_bucket_page_id);
  raw_old_bucket_page->WLatch();
  auto *old_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_old_bucket_page->GetData());
  uint32_t old_index = KeyToDirectoryIndex(key, dir_page);
  bool insert_succeed = false;
  bool insert_finished = false;
  if (!old_bucket_page->IsFull()) {
    insert_succeed = old_bucket_page->Insert(key, value, comparator_);
    raw_old_bucket_page->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return insert_succeed;
  }
  std::vector<ValueType> res;
  old_bucket_page->GetValue(key, comparator_, &res);
  if (!res.empty()) {
    for (const auto &val : res) {
      if (val == value) {
        raw_old_bucket_page->WUnlatch();
        table_latch_.RUnlock();
        buffer_pool_manager_->UnpinPage(directory_page_id_, false);
        buffer_pool_manager_->UnpinPage(old_bucket_page_id, false);
        return false;
      }
    }
  }
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *insert_page;
  page_id_t insert_page_id;
  Page *raw_insert_page;
  while (!insert_finished) {
    if (dir_page->GetLocalDepth(old_index) >= dir_page->GetGlobalDepth()) {
      uint32_t prev_size = dir_page->Size();
      dir_page->IncrGlobalDepth();
      uint32_t cur_size = dir_page->Size();
      for (uint32_t i = prev_size; i < cur_size; ++i) {
        uint32_t image_index = i - prev_size;
        dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_index));
        dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(image_index));
      }
    }
    page_id_t new_bucket_page_id;
    Page *raw_new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
    raw_new_bucket_page->WLatch();
    auto new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_new_bucket_page->GetData());
    uint32_t split_index = dir_page->GetSplitImageIndex(old_index);
    dir_page->IncrLocalDepth(old_index);
    uint32_t local_depth_mask = dir_page->GetLocalDepthMask(old_index);
    for (uint32_t i = 0; i < dir_page->Size(); ++i) {
      if ((i & local_depth_mask) == (old_index & local_depth_mask)) {
        if (i != old_index) {
          dir_page->IncrLocalDepth(i);
        }
      }
      if ((i & local_depth_mask) == (split_index & local_depth_mask)) {
        dir_page->IncrLocalDepth(i);
        dir_page->SetBucketPageId(i, new_bucket_page_id);
      }
    }
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
      if (old_bucket_page->IsReadable(i)) {
        KeyType tmp_key = old_bucket_page->KeyAt(i);
        uint32_t tmp_index = KeyToDirectoryIndex(tmp_key, dir_page);
        if ((local_depth_mask & tmp_index) == split_index) {
          ValueType tmp_value = old_bucket_page->ValueAt(i);
          old_bucket_page->RemoveAt(i);
          new_bucket_page->Insert(tmp_key, tmp_value, comparator_);
        }
      }
    }
    uint32_t new_index = KeyToDirectoryIndex(key, dir_page);
    if (dir_page->GetBucketPageId(new_index) == old_bucket_page_id) {
      insert_page = old_bucket_page;
      insert_page_id = old_bucket_page_id;
      raw_insert_page = raw_old_bucket_page;
      raw_new_bucket_page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));
    } else {
      insert_page = new_bucket_page;
      insert_page_id = new_bucket_page_id;
      raw_insert_page = raw_new_bucket_page;
      raw_old_bucket_page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(old_bucket_page_id, true));
    }
    if (!insert_page->IsFull()) {
      insert_succeed = insert_page->Insert(key, value, comparator_);
      insert_finished = true;
      raw_insert_page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(insert_page_id, true));
    } else {  // after split page, because of the imbalance, we need split the page again.
      old_bucket_page = insert_page;
      raw_old_bucket_page = raw_insert_page;
      old_bucket_page_id = insert_page_id;
      old_index = new_index;
    }
  }
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  return insert_succeed;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool is_merge = false;
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  auto *raw_bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  raw_bucket_page->WLatch();
  auto *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_bucket_page->GetData());
  bool remove_succeed = bucket_page->Remove(key, value, comparator_);
  if (bucket_page->IsEmpty() && dir_page->GetGlobalDepth() && dir_page->GetLocalDepth(bucket_index)) {
    uint32_t merge_index = dir_page->GetMergeImageIndex(bucket_index);
    if (dir_page->GetLocalDepth(merge_index) == dir_page->GetLocalDepth(bucket_index)) {
      is_merge = true;
      table_latch_.RUnlock();
      raw_bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      Merge(transaction, key, value);
    }
  }
  if (!is_merge) {
    table_latch_.RUnlock();
    raw_bucket_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  }
  return remove_succeed;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  auto *raw_bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  raw_bucket_page->WLatch();
  auto *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_bucket_page->GetData());
  if (!bucket_page->IsEmpty() || !dir_page->GetLocalDepth(bucket_index)) {
    raw_bucket_page->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return;
  }
  uint32_t merge_index = dir_page->GetMergeImageIndex(bucket_index);
  page_id_t merge_bucket_page_id = dir_page->GetBucketPageId(merge_index);
  if (!bucket_page->IsEmpty() || !dir_page->GetGlobalDepth() || !dir_page->GetLocalDepth(bucket_index) ||
      (dir_page->GetLocalDepth(bucket_index) != dir_page->GetLocalDepth(merge_index)) ||
      (bucket_page_id == merge_bucket_page_id)) {
    raw_bucket_page->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return;
  }
  dir_page->DecrLocalDepth(bucket_index);
  dir_page->DecrLocalDepth(merge_index);
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->SetBucketPageId(i, merge_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(merge_index));
    } else if (dir_page->GetBucketPageId(i) == merge_bucket_page_id) {
      if (i != merge_index) {
        dir_page->DecrLocalDepth(i);
      }
    }
  }
  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  raw_bucket_page->WUnlatch();
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  Merge(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::TestInterface() {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  uint32_t num_readable = 0;
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    auto raw_bucket_page = buffer_pool_manager_->FetchPage(page_id);
    auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_bucket_page->GetData());
    bool is_empty = bucket_page->IsEmpty();
    LOG_DEBUG("page id %u, is empty %d", page_id, is_empty);
    num_readable += bucket_page->NumReadable();
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  LOG_DEBUG("num readable %u", num_readable);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
