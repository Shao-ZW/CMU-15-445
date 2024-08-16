//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t fid = -1;

  latch_.lock();
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    bool evict_success = replacer_->Evict(&fid);
    if (evict_success) {
      if (pages_[fid].is_dirty_) {
        disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      }
      page_table_.erase(pages_[fid].page_id_);
    }
  }

  if (fid != -1) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    page_id_t pid = AllocatePage();
    pages_[fid].ResetMemory();
    pages_[fid].pin_count_++;
    pages_[fid].page_id_ = pid;
    pages_[fid].is_dirty_ = false;
    page_table_[pid] = fid;
    *page_id = pid;
    latch_.unlock();
    return pages_ + fid;
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t fid = -1;

  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    fid = it->second;
  } else {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
      disk_manager_->ReadPage(page_id, pages_[fid].data_);
    } else {
      bool evict_success = replacer_->Evict(&fid);
      if (evict_success) {
        if (pages_[fid].is_dirty_) {
          disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
          pages_[fid].is_dirty_ = false;
        }
        page_table_.erase(pages_[fid].page_id_);
        disk_manager_->ReadPage(page_id, pages_[fid].data_);
      }
    }
  }

  if (fid != -1) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    pages_[fid].pin_count_++;
    pages_[fid].page_id_ = page_id;
    page_table_[page_id] = fid;
    latch_.unlock();
    return pages_ + fid;
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    if (pages_[fid].pin_count_ <= 0) {
      latch_.unlock();
      return false;
    }

    pages_[fid].pin_count_--;
    if (is_dirty) {
      pages_[fid].is_dirty_ = is_dirty;
    }
    if (pages_[fid].pin_count_ == 0) {
      replacer_->SetEvictable(fid, true);
    }
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    disk_manager_->WritePage(page_id, pages_[fid].data_);
    pages_[fid].is_dirty_ = false;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (auto item : page_table_) {
    FlushPage(item.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    if (pages_[fid].pin_count_ > 0) {
      latch_.unlock();
      return false;
    }
    replacer_->Remove(fid);
    page_table_.erase(page_id);
    free_list_.push_back(fid);
    pages_[fid].ResetMemory();
    pages_[fid].is_dirty_ = false;
    pages_[fid].pin_count_ = 0;
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    DeallocatePage(page_id);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
