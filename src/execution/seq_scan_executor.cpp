//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = std::make_shared<TableIterator>(
      table_info_->table_->MakeEagerIterator());  // this will have dead loops when updating while scanning (to be done)
  try {
    if (exec_ctx_->IsDelete()) {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                  LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
        throw ExecutionException("Seq ScanExecutor Get Table Lock Failed");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
        if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                    LockManager::LockMode::INTENTION_SHARED, table_info_->oid_)) {
          throw ExecutionException("Seq ScanExecutor Get Table Lock Failed");
        }
      }
    }
  } catch (TransactionAbortException &except) {
    throw ExecutionException("Seq ScanExecutor Get Table Lock Failed");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iterator_->IsEnd()) {
    *rid = table_iterator_->GetRID();
    try {
      if (exec_ctx_->IsDelete()) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  plan_->table_oid_, *rid)) {
          throw ExecutionException("Seq ScanExecutor Get Row Lock Failed");
        }
      } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(table_info_->oid_, *rid)) {
          if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                    plan_->table_oid_, *rid)) {
            throw ExecutionException("Seq ScanExecutor Get Row Lock Failed");
          }
        }
      }
    } catch (TransactionAbortException &except) {
      throw ExecutionException(except.GetInfo());
    }

    auto [meta, new_tuple] = table_iterator_->GetTuple();
    ++(*table_iterator_);
    bool can_read = !meta.is_deleted_;

    if (!can_read) {
      if (exec_ctx_->IsDelete() ||
          exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid, true)) {
            throw ExecutionException("Seq ScanExecutor Release Row Lock Failed");
          }
        } catch (TransactionAbortException &except) {
          throw ExecutionException("Seq ScanExecutor Release Row Lock Failed");
        }
      }
    } else {
      *tuple = new_tuple;
      if (!exec_ctx_->IsDelete() &&
          exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid)) {
            throw ExecutionException("Seq ScanExecutor Release Row Lock Failed");
          }
        } catch (TransactionAbortException &except) {
          throw ExecutionException("Seq ScanExecutor Release Row Lock Failed");
        }
      }
      return true;
    }
  }

  return false;
}

}  // namespace bustub
