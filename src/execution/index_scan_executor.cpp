//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "storage/index/index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  table_info_ = catalog->GetTable(index_info->table_name_);
  index_iterator_ = std::make_shared<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iterator_->IsEnd()) {
    return false;
  }

  auto [key, rid_t] = **index_iterator_;
  auto [meta, tuple_t] = table_info_->table_->GetTuple(rid_t);
  while (meta.is_deleted_) {
    ++(*index_iterator_);
    if (index_iterator_->IsEnd()) {
      return false;
    }
    std::tie(key, rid_t) = **index_iterator_;
    std::tie(meta, tuple_t) = table_info_->table_->GetTuple(rid_t);
  }

  *tuple = tuple_t;
  *rid = rid_t;
  ++(*index_iterator_);
  return true;
}

}  // namespace bustub
