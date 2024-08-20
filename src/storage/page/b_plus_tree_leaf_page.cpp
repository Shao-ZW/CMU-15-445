//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index key access in b+-tree leaf page");
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetMaxSize()) {
    throw bustub::Exception("invalid index value access in b+-tree leaf page");
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKV(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &compare) const -> int {
  int leaf_size = GetSize();
  auto pos = std::lower_bound(array_, array_ + leaf_size, key,
                              [&compare](const auto &pair, const auto &key) { return compare(pair.first, key) < 0; });

  if (pos != array_ + leaf_size && compare(pos->first, key) == 0) {
    return pos - array_;
  }

  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Move(int begin_index, int end_index, BPlusTreeLeafPage *dst, int dst_index) {
  std::copy(array_ + begin_index, array_ + end_index, dst->array_ + dst_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &compare) {
  int leaf_size = GetSize();
  auto pos = std::lower_bound(array_, array_ + leaf_size, key,
                              [&compare](const auto &pair, const auto &key) { return compare(pair.first, key) < 0; });

  if (pos != array_ + leaf_size && compare(pos->first, key) == 0) {
    return;
  }

  std::copy_backward(pos, array_ + leaf_size, array_ + leaf_size + 1);
  *pos = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(const KeyType &key, const ValueType &value) {
  int leaf_size = GetSize();

  if (leaf_size > 0) {
    std::copy_backward(array_, array_ + leaf_size, array_ + leaf_size + 1);
  }
  array_[0] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value) {
  int leaf_size = GetSize();
  array_[leaf_size] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &compare) {
  int leaf_size = GetSize();
  auto pos = std::lower_bound(array_, array_ + leaf_size, key,
                              [&compare](const auto &pair, const auto &key) { return compare(pair.first, key) < 0; });
  if (pos != array_ + leaf_size && compare(pos->first, key) == 0) {
    std::copy(pos + 1, array_ + leaf_size, pos);
    IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(int index) {
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
