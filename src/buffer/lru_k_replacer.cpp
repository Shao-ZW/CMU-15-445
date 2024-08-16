//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : fid_(fid), k_(k) {}

void LRUKNode::RecordUpdate(size_t curr_time) {
  history_.push_back(curr_time);

  while (history_.size() > k_) {
    history_.pop_front();
  }
}

auto LRUKNode::CalTime(size_t curr_time) -> std::pair<int, size_t> {
  std::pair<int, size_t> res{0, 0};

  if (history_.size() < k_) {
    res.first = 1;
  }

  res.second = curr_time - history_.front();
  return res;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    return false;
  }

  frame_id_t evict_id;
  std::pair<int, size_t> evict_time{0, 0};

  latch_.lock();
  for (auto &item : node_store_) {
    LRUKNode &node = item.second;
    if (node.is_evictable_) {
      std::pair<int, size_t> c_time = node.CalTime(current_timestamp_);
      if ((evict_time.first < c_time.first) ||
          (evict_time.first == c_time.first && evict_time.second < c_time.second)) {
        evict_time = c_time;
        evict_id = item.first;
      }
    }
  }
  node_store_.erase(evict_id);
  curr_size_--;
  latch_.unlock();

  *frame_id = evict_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  current_timestamp_++;

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.insert({frame_id, LRUKNode(frame_id, k_)});
    node_store_[frame_id].RecordUpdate(current_timestamp_);
    latch_.unlock();
    return;
  }

  node_store_[frame_id].RecordUpdate(current_timestamp_);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw bustub::Exception("cann't find target frame id in lruk replacer");
  }

  if (set_evictable && !node_store_[frame_id].is_evictable_) {
    curr_size_++;
  } else if (!set_evictable && node_store_[frame_id].is_evictable_) {
    curr_size_--;
  }

  node_store_[frame_id].is_evictable_ = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    if (it->second.is_evictable_) {
      curr_size_--;
    }
    node_store_.erase(frame_id);
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
