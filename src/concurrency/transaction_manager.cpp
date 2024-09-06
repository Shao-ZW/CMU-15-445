//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
  std::scoped_lock lock(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_set = txn->GetWriteSet();
  while (!table_write_set->empty()) {
    auto &record = table_write_set->back();
    auto tuple_meta = record.table_heap_->GetTupleMeta(record.rid_);
    switch (record.wtype_) {
      case WType::INSERT:
        tuple_meta.is_deleted_ = true;
        record.table_heap_->UpdateTupleMeta(tuple_meta, record.rid_);
        break;
      case WType::DELETE:
        tuple_meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(tuple_meta, record.rid_);
        break;
      case WType::UPDATE:
        break;
    }
    table_write_set->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
  std::scoped_lock lock(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
