//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index access in b+-tree internal page");
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index access in b+-tree internal page");
  }
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index access in b+-tree internal page");
  }
  array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index access in b+-tree internal page");
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &compare) const -> int {
  auto pos = std::upper_bound(array_ + 1, array_ + GetSize(), key,
                              [&compare](const auto &key, const auto &pair) { return compare(key, pair.first) < 0; });
  pos--;
  return pos - array_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Move(int begin_index, int end_index, BPlusTreeInternalPage *dst, int dst_index) {
  std::copy(array_ + begin_index, array_ + end_index, dst->array_ + dst_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushFront(const KeyType &key, const ValueType &value) {
  int internal_size = GetSize();

  if (internal_size > 0) {
    std::copy_backward(array_, array_ + internal_size, array_ + internal_size + 1);
  }
  array_[0] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value) {
  int internal_size = GetSize();
  array_[internal_size] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &compare) {
  int internal_size = GetSize();

  if (internal_size == 1) {
    array_[1] = {key, value};
  } else {
    auto pos = std::lower_bound(array_ + 1, array_ + internal_size, key,
                                [&compare](const auto &pair, const auto &key) { return compare(pair.first, key) < 0; });
    std::copy_backward(pos, array_ + internal_size, array_ + internal_size + 1);
    *pos = {key, value};
  }

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int index) {
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
