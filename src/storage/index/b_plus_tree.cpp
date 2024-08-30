#include <cassert>
#include <ios>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  page_id_t root_page_id = GetRootPageId();

  if (root_page_id == INVALID_PAGE_ID) {
    return true;
  }

  auto root_page_rguard = bpm_->FetchPageRead(root_page_id);
  auto root_page = root_page_rguard.As<BPlusTreePage>();
  return root_page->GetSize() == 0;
}

/*
 * Helper function to find the key correspond leaf pageid (only acquire read latch)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReadTraverseToLeaf(const KeyType &key, Context &ctx, bool &key_in_leaf, BPlusOpType type) const
    -> page_id_t {
  auto page_rguard = bpm_->FetchPageRead(header_page_id_);
  page_id_t curr_page_id = page_rguard.As<BPlusTreeHeaderPage>()->root_page_id_;

  if (curr_page_id == INVALID_PAGE_ID) {
    key_in_leaf = false;
    return INVALID_PAGE_ID;
  }

  ctx.read_set_.push_back(std::move(page_rguard));
  page_rguard = bpm_->FetchPageRead(curr_page_id);
  auto curr_page = page_rguard.As<InternalPage>();

  while (!curr_page->IsLeafPage()) {
    ctx.read_set_.push_back(std::move(page_rguard));
    curr_page_id = curr_page->ValueAt(curr_page->KeyIndex(key, comparator_));
    page_rguard = bpm_->FetchPageRead(curr_page_id);
    curr_page = page_rguard.As<InternalPage>();
    ctx.read_set_.pop_front();
  }

  auto leaf_page = page_rguard.As<LeafPage>();
  key_in_leaf = leaf_page->KeyIndex(key, comparator_) != -1;

  if (type == BPlusOpType::Find) {
    ctx.read_set_.clear();
    ctx.read_set_.push_back(std::move(page_rguard));
  } else if (type == BPlusOpType::Insert || type == BPlusOpType::Delete) {
    page_rguard.Drop();
    ctx.write_set_.push_back(bpm_->FetchPageWrite(curr_page_id));
    ctx.read_set_.clear();
  }

  return curr_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::WriteTraverseToLeaf(const KeyType &key, Context &ctx, bool &key_in_leaf, BPlusOpType type) const
    -> page_id_t {
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  page_id_t curr_page_id = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  // no root !
  if (curr_page_id == INVALID_PAGE_ID) {
    key_in_leaf = false;
    return INVALID_PAGE_ID;
  }

  bool is_root_change = true;
  ctx.root_page_id_ = curr_page_id;
  auto curr_page_wguard = bpm_->FetchPageWrite(curr_page_id);
  auto curr_page = curr_page_wguard.As<InternalPage>();

  if (curr_page->IsLeafPage() && (type == BPlusOpType::Delete || curr_page->IsSafe(BPlusOpType::Insert))) {
    is_root_change = false;
  }

  while (!curr_page->IsLeafPage()) {
    ctx.write_set_.push_back(std::move(curr_page_wguard));
    curr_page_id = curr_page->ValueAt(curr_page->KeyIndex(key, comparator_));
    curr_page_wguard = bpm_->FetchPageWrite(curr_page_id);
    curr_page = curr_page_wguard.As<InternalPage>();
    if (curr_page->IsSafe(type)) {
      ctx.write_set_.clear();
      is_root_change = false;
    }
  }

  auto leaf_page = curr_page_wguard.As<LeafPage>();
  key_in_leaf = leaf_page->KeyIndex(key, comparator_) != -1;
  ctx.write_set_.push_back(std::move(curr_page_wguard));

  if (!is_root_change) {
    ctx.header_page_ = std::nullopt;
  }

  return curr_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
void BPLUSTREE_TYPE::RootSplit(PageType *old_root_page, page_id_t old_root_page_id, Context &ctx) {
  page_id_t new_root_page_id;
  auto new_root_page_wguard = bpm_->NewPageWrite(&new_root_page_id);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;
  auto new_root_page = new_root_page_wguard.AsMut<InternalPage>();
  new_root_page->Init(internal_max_size_);
  new_root_page->SetSize(1);
  new_root_page->SetValueAt(0, old_root_page_id);
  Split(new_root_page, old_root_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(InternalPage *parent_page, LeafPage *leaf_page) {
  if (!leaf_page->IsOverFlow()) {
    return;
  }

  page_id_t new_page_id;
  auto new_page_wguard = bpm_->NewPageWrite(&new_page_id);
  auto new_leaf_page = new_page_wguard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  int leaf_size = leaf_page->GetSize();
  int split_index = leaf_size / 2;
  int copy_size = leaf_size - split_index;

  leaf_page->Move(split_index, leaf_size, new_leaf_page, 0);
  leaf_page->IncreaseSize(-copy_size);
  new_leaf_page->IncreaseSize(copy_size);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  parent_page->Insert(new_leaf_page->KeyAt(0), new_page_id, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(InternalPage *parent_page, InternalPage *internal_page) {
  if (!internal_page->IsOverFlow()) {
    return;
  }

  page_id_t new_page_id;
  auto new_page_wguard = bpm_->NewPageWrite(&new_page_id);
  auto new_internal_page = new_page_wguard.AsMut<InternalPage>();
  new_internal_page->Init(internal_max_size_);

  int internal_size = internal_page->GetSize();
  int split_index = internal_size / 2;
  int copy_size = internal_size - split_index;

  parent_page->Insert(internal_page->KeyAt(split_index), new_page_id, comparator_);
  internal_page->Move(split_index, internal_size, new_internal_page, 0);
  internal_page->IncreaseSize(-copy_size);
  new_internal_page->IncreaseSize(copy_size);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(InternalPage *parent_page, LeafPage *lchild_page, LeafPage *rchild_page, int modify_index) {
  rchild_page->Move(0, rchild_page->GetSize(), lchild_page, lchild_page->GetSize());
  lchild_page->IncreaseSize(rchild_page->GetSize());
  lchild_page->SetNextPageId(rchild_page->GetNextPageId());
  bpm_->DeletePage(parent_page->ValueAt(modify_index));
  parent_page->Delete(modify_index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(InternalPage *parent_page, InternalPage *lchild_page, InternalPage *rchild_page,
                           int modify_index) {
  rchild_page->Move(0, rchild_page->GetSize(), lchild_page, lchild_page->GetSize());
  lchild_page->SetKeyAt(lchild_page->GetSize(), parent_page->KeyAt(modify_index));
  lchild_page->IncreaseSize(rchild_page->GetSize());
  bpm_->DeletePage(parent_page->ValueAt(modify_index));
  parent_page->Delete(modify_index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *parent_page, LeafPage *lchild_page, LeafPage *rchild_page,
                                  int modify_index) {
  int lchild_size = lchild_page->GetSize();
  int rchild_size = rchild_page->GetSize();

  if (lchild_size < rchild_size) {
    lchild_page->PushBack(rchild_page->KeyAt(0), rchild_page->ValueAt(0));
    rchild_page->Delete(0);
  } else {
    rchild_page->PushFront(lchild_page->KeyAt(lchild_size - 1), lchild_page->ValueAt(lchild_size - 1));
    lchild_page->Delete(lchild_size - 1);
  }

  parent_page->SetKeyAt(modify_index, rchild_page->KeyAt(0));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *parent_page, InternalPage *lchild_page, InternalPage *rchild_page,
                                  int modify_index) {
  int lchild_size = lchild_page->GetSize();
  int rchild_size = rchild_page->GetSize();

  if (lchild_size < rchild_size) {
    lchild_page->PushBack(parent_page->KeyAt(modify_index), rchild_page->ValueAt(0));
    parent_page->SetKeyAt(modify_index, rchild_page->KeyAt(1));
    rchild_page->Delete(0);
  } else {
    rchild_page->PushFront(parent_page->KeyAt(modify_index), lchild_page->ValueAt(lchild_size - 1));
    rchild_page->SetKeyAt(1, parent_page->KeyAt(modify_index));
    parent_page->SetKeyAt(modify_index, lchild_page->KeyAt(lchild_size - 1));
    lchild_page->Delete(lchild_size - 1);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  bool key_in_leaf;
  Context ctx;

  page_id_t leaf_page_id = ReadTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Find);

  if (leaf_page_id == INVALID_PAGE_ID || !key_in_leaf) {
    return false;
  }

  auto &leaf_page_rguard = ctx.read_set_.back();
  auto leaf_page = leaf_page_rguard.As<LeafPage>();
  int index = leaf_page->KeyIndex(key, comparator_);
  result->push_back(leaf_page->ValueAt(index));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // try optimistically first (wish only modify leaf node)
  {
    Context ctx;
    bool key_in_leaf;
    page_id_t leaf_page_id = ReadTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Insert);

    if (key_in_leaf) {
      return false;
    }

    if (leaf_page_id != INVALID_PAGE_ID) {
      auto &leaf_page_wguard = ctx.write_set_.back();
      auto leaf_page = leaf_page_wguard.AsMut<LeafPage>();
      if (leaf_page->IsSafe(BPlusOpType::Insert)) {
        leaf_page->Insert(key, value, comparator_);
        return true;
      }
    }
  }

  // pessimistic algorithm now
  {
    Context ctx;
    bool key_in_leaf;
    page_id_t leaf_page_id = WriteTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Insert);

    if (key_in_leaf) {
      return false;
    }

    // no root ?
    if (leaf_page_id == INVALID_PAGE_ID) {
      page_id_t new_root_page_id;
      auto new_root_page_wguard = bpm_->NewPageWrite(&new_root_page_id);
      auto new_root_page = new_root_page_wguard.AsMut<LeafPage>();
      new_root_page->Init(leaf_max_size_);
      new_root_page->Insert(key, value, comparator_);
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_page_id;
      return true;
    }

    auto &leaf_page_wguard = ctx.write_set_.back();
    auto leaf_page = leaf_page_wguard.AsMut<LeafPage>();

    leaf_page->Insert(key, value, comparator_);

    // leaf root split ?
    if (ctx.IsRootPage(leaf_page_id) && leaf_page->IsOverFlow()) {
      RootSplit(leaf_page, ctx.root_page_id_, ctx);
      return true;
    }

    if (ctx.write_set_.size() > 1) {
      auto parent_page = (ctx.write_set_.rbegin() + 1)->AsMut<InternalPage>();
      Split(parent_page, leaf_page);
      ctx.write_set_.pop_back();

      while (parent_page->IsOverFlow() && ctx.write_set_.size() > 1) {
        auto curr_page = parent_page;
        parent_page = (ctx.write_set_.rbegin() + 1)->AsMut<InternalPage>();
        Split(parent_page, curr_page);
        ctx.write_set_.pop_back();
      }

      // internal root split ?
      if (ctx.IsRootPage(ctx.write_set_.back().PageId()) && parent_page->IsOverFlow()) {
        RootSplit(parent_page, ctx.root_page_id_, ctx);
      }
    }

    return true;
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // try optimistically first (wish only modify leaf node)
  {
    Context ctx;
    bool key_in_leaf;
    page_id_t leaf_page_id = ReadTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Delete);

    if (leaf_page_id == INVALID_PAGE_ID || !key_in_leaf) {
      return;
    }

    auto &leaf_page_wguard = ctx.write_set_.back();
    auto leaf_page = leaf_page_wguard.AsMut<LeafPage>();
    if (leaf_page->IsSafe(BPlusOpType::Delete)) {
      leaf_page->Delete(key, comparator_);
      return;
    }
  }

  // pessimistic algorithm now
  {
    Context ctx;
    bool key_in_leaf;
    page_id_t leaf_page_id = WriteTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Delete);

    if (leaf_page_id == INVALID_PAGE_ID || !key_in_leaf) {
      return;
    }

    auto &leaf_page_wguard = ctx.write_set_.back();
    auto leaf_page = leaf_page_wguard.AsMut<LeafPage>();
    leaf_page->Delete(key, comparator_);

    // leaf is root ?
    if (ctx.IsRootPage(leaf_page_id)) {
      return;
    }

    if (ctx.write_set_.size() > 1) {
      auto &parent_page_wguard = *(ctx.write_set_.rbegin() + 1);
      auto parent_page = parent_page_wguard.AsMut<InternalPage>();
      int index = parent_page->KeyIndex(key, comparator_);

      // try borrow from sibling leaf first
      if (index > 0) {  // left sibling
        page_id_t sibling_page_id = parent_page->ValueAt(index - 1);
        auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
        auto sibling_page = sibling_page_wguard.AsMut<LeafPage>();
        if (sibling_page->IsSafe(BPlusOpType::Delete)) {
          Redistribute(parent_page, sibling_page, leaf_page, index);
          return;
        }
      }

      if (index < parent_page->GetSize() - 1) {  // right sibling
        page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
        auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
        auto sibling_page = sibling_page_wguard.AsMut<LeafPage>();
        if (sibling_page->IsSafe(BPlusOpType::Delete)) {
          Redistribute(parent_page, leaf_page, sibling_page, index + 1);
          return;
        }
      }

      // merge sibling leaf
      if (index > 0) {  // left sibling
        page_id_t sibling_page_id = parent_page->ValueAt(index - 1);
        auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
        auto sibling_page = sibling_page_wguard.AsMut<LeafPage>();
        Merge(parent_page, sibling_page, leaf_page, index);  // NOLINT
      } else if (index < parent_page->GetSize() - 1) {       // right sibling
        page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
        auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
        auto sibling_page = sibling_page_wguard.AsMut<LeafPage>();
        Merge(parent_page, leaf_page, sibling_page, index + 1);
      }

      ctx.write_set_.pop_back();

      while (parent_page->IsUnderFlow() && ctx.write_set_.size() > 1) {
        auto curr_page = parent_page;
        parent_page = (ctx.write_set_.rbegin() + 1)->AsMut<InternalPage>();
        index = parent_page->KeyIndex(key, comparator_);

        // try redistube first
        if (index > 0) {  // left sibling
          page_id_t sibling_page_id = parent_page->ValueAt(index - 1);
          auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
          auto sibling_page = sibling_page_wguard.AsMut<InternalPage>();
          if (sibling_page->IsSafe(BPlusOpType::Delete)) {
            Redistribute(parent_page, sibling_page, curr_page, index);
            return;
          }
        }

        if (index < parent_page->GetSize() - 1) {  // right sibling
          page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
          auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
          auto sibling_page = sibling_page_wguard.AsMut<InternalPage>();
          if (sibling_page->IsSafe(BPlusOpType::Delete)) {
            Redistribute(parent_page, curr_page, sibling_page, index + 1);
            return;
          }
        }

        // merge sibling internal
        if (index > 0) {  // left sibling
          page_id_t sibling_page_id = parent_page->ValueAt(index - 1);
          auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
          auto sibling_page = sibling_page_wguard.AsMut<InternalPage>();
          Merge(parent_page, sibling_page, curr_page, index);  // NOLINT
        } else if (index < parent_page->GetSize() - 1) {       // right sibling
          page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
          auto sibling_page_wguard = bpm_->FetchPageWrite(sibling_page_id);
          auto sibling_page = sibling_page_wguard.AsMut<InternalPage>();
          Merge(parent_page, curr_page, sibling_page, index + 1);
        }

        ctx.write_set_.pop_back();
      }

      // internal root is empty ?
      page_id_t parent_page_id = ctx.write_set_.back().PageId();
      if (ctx.IsRootPage(parent_page_id) && parent_page->GetSize() == 1) {
        auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = parent_page->ValueAt(0);
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  int index = 0;

  if (!IsEmpty()) {
    auto page_rguard = bpm_->FetchPageRead(header_page_id_);
    page_id_t curr_page_id = page_rguard.As<BPlusTreeHeaderPage>()->root_page_id_;
    page_rguard = bpm_->FetchPageRead(curr_page_id);
    auto curr_page = page_rguard.As<InternalPage>();

    while (!curr_page->IsLeafPage()) {
      curr_page_id = curr_page->ValueAt(0);
      page_rguard = bpm_->FetchPageRead(curr_page_id);
      curr_page = page_rguard.As<InternalPage>();
    }

    leaf_page_id = curr_page_id;
  }

  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, index);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  bool key_in_leaf;
  page_id_t leaf_page_id = ReadTraverseToLeaf(key, ctx, key_in_leaf, BPlusOpType::Find);
  int index = 0;

  if (leaf_page_id != INVALID_PAGE_ID && key_in_leaf) {
    auto &leaf_page_rguard = ctx.read_set_.back();
    auto leaf_page = leaf_page_rguard.As<LeafPage>();
    index = leaf_page->KeyIndex(key, comparator_);
  }

  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto header_page_rguard = bpm_->FetchPageRead(header_page_id_);
  return header_page_rguard.As<BPlusTreeHeaderPage>()->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
