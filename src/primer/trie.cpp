#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> ptr = root_;

  for (char ch : key) {
    if (!ptr) {
      return nullptr;
    }
    auto it = ptr->children_.find(ch);
    if (it == ptr->children_.end()) {
      return nullptr;
    }
    ptr = it->second;
  }

  auto nwv = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());

  return nwv ? nwv->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  auto value_ptr = std::make_shared<T>(std::move(value));
  std::shared_ptr<TrieNode> new_root;

  if (key.empty()) {
    new_root = root_ ? std::make_shared<TrieNodeWithValue<T>>(root_->children_, value_ptr)
                     : std::make_shared<TrieNodeWithValue<T>>(value_ptr);
    return Trie(new_root);
  }

  std::unique_ptr<TrieNode> tmp = root_ ? root_->Clone() : std::make_unique<TrieNode>();
  std::shared_ptr<TrieNode> ptr = std::shared_ptr<TrieNode>(std::move(tmp));
  std::shared_ptr<TrieNode> clone_node;
  std::shared_ptr<TrieNode> new_node;
  size_t idx = 0;
  new_root = ptr;

  while (idx < key.size() - 1) {
    auto it = ptr->children_.find(key[idx]);
    if (it == ptr->children_.end()) {
      break;
    }
    tmp = it->second->Clone();
    clone_node = std::shared_ptr<TrieNode>(std::move(tmp));
    ptr->children_[key[idx]] = clone_node;
    ptr = clone_node;
    idx++;
  }

  while (idx < key.size() - 1) {
    new_node = std::make_shared<TrieNode>();
    ptr->children_[key[idx]] = new_node;
    ptr = new_node;
    idx++;
  }

  auto it = ptr->children_.find(key[idx]);
  if (it == ptr->children_.end()) {
    new_node = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
  } else {
    new_node = std::make_shared<TrieNodeWithValue<T>>(it->second->children_, value_ptr);
  }
  ptr->children_[key[idx]] = new_node;

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::vector<std::shared_ptr<const TrieNode>> path;
  std::shared_ptr<const TrieNode> ptr = root_;

  for (char ch : key) {
    if (!ptr) {
      return Trie(root_);
    }
    auto it = ptr->children_.find(ch);
    if (it == ptr->children_.end()) {
      return Trie(root_);
    }
    path.push_back(ptr);
    ptr = it->second;
  }

  if (!ptr->is_value_node_) {
    return Trie(root_);
  }

  bool is_delete = ptr->children_.empty();
  int idx = path.size() - 1;

  if (!is_delete) {
    ptr = std::make_shared<TrieNode>(ptr->children_);
  } else {
    while (idx >= 0 && path[idx]->children_.size() == 1 && !path[idx]->is_value_node_) {
      idx--;
    }
    ptr = nullptr;
  }

  while (idx >= 0) {
    std::unique_ptr<TrieNode> fa = path[idx]->Clone();
    if (ptr) {
      fa->children_[key[idx]] = ptr;
    } else {
      fa->children_.erase(key[idx]);
    }
    ptr = std::shared_ptr<TrieNode>(std::move(fa));
    idx--;
  }

  return Trie(ptr);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
