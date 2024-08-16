#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (bpm_ != nullptr && page_ != nullptr && page_ != that.page_) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }

  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr) {
    guard_.Drop();
    guard_.page_->RUnlatch();
  }
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }

  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr) {
    guard_.Drop();
    guard_.page_->WUnlatch();
  }
}

WritePageGuard::~WritePageGuard() {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
  }
}  // NOLINT

}  // namespace bustub
