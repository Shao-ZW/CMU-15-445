//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

auto BPlusTreePage::IsOverFlow() const -> bool { return GetSize() > GetMaxSize(); }

auto BPlusTreePage::IsUnderFlow() const -> bool { return GetSize() < GetMinSize(); }

auto BPlusTreePage::IsSafe(BPlusOpType type) const -> bool {
  if (type == BPlusOpType::Insert) {
    return GetSize() < GetMaxSize();
  }

  if (type == BPlusOpType::Delete) {
    return GetSize() > GetMinSize();
  }
  return true;
}

/*
 * Helper method to get min page size
 */
auto BPlusTreePage::GetMinSize() const -> int {
  return page_type_ == IndexPageType::LEAF_PAGE ? max_size_ / 2 : (max_size_ + 1) / 2;
}

}  // namespace bustub
