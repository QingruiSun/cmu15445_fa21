//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(static_cast<uint32_t>(num_instances)),
      pool_size_(pool_size),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  allocate_index_ = 0;
  // Allocate and create individual BufferPoolManagerInstances
  bpms_ = new BufferPoolManagerInstance *[num_instances_];
  for (uint32_t i = 0; i < num_instances; ++i) {
    bpms_[i] = new BufferPoolManagerInstance(pool_size, num_instances_, i, disk_manager_, log_manager_);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (uint32_t i = 0; i < num_instances_; ++i) {
    free(bpms_[i]);
  }
  free(bpms_);
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * static_cast<size_t>(num_instances_);
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return bpms_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::scoped_lock scoped_latch(latch_);
  uint32_t start_allocate_index = allocate_index_;
  Page *page;
  if ((page = bpms_[allocate_index_]->NewPage(page_id)) != nullptr) {
    allocate_index_ = (allocate_index_ + 1) % num_instances_;
    return page;
  }
  allocate_index_ = (allocate_index_ + 1) % num_instances_;
  while (allocate_index_ != start_allocate_index) {
    if ((page = bpms_[allocate_index_]->NewPage(page_id)) != nullptr) {
      allocate_index_ = (allocate_index_ + 1) % num_instances_;
      return page;
    }
    allocate_index_ = (allocate_index_ + 1) % num_instances_;
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (uint32_t i = 0; i < num_instances_; ++i) {
    bpms_[i]->FlushAllPages();
  }
}

}  // namespace bustub
