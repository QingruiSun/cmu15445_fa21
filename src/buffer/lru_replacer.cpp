//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_list_.empty()) {
    return false;
  }
  *frame_id = frame_list_.back();
  frame_list_.pop_back();
  frame_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    frame_list_.erase(frame_map_[frame_id]);
    frame_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_list_.size() >= num_pages_) {
    return;
  }
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    return;
  }
  frame_list_.push_front(frame_id);
  frame_map_[frame_id] = frame_list_.begin();
}

size_t LRUReplacer::Size() {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  return frame_list_.size();
}

}  // namespace bustub
