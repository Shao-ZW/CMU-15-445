//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "common/config.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  is_end_ = false;
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                plan_->TableOid())) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &except) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple child_tuple;
  int insert_cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    auto child_rid = table_info_->table_->InsertTuple(
        {exec_ctx_->GetTransaction()->GetTransactionId(), INVALID_TXN_ID, false}, child_tuple,
        exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    if (child_rid != std::nullopt) {
      try {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  table_info_->oid_, *child_rid)) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException &except) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }

      auto table_record = TableWriteRecord(table_info_->oid_, child_rid.value(), table_info_->table_.get());
      table_record.wtype_ = WType::INSERT;
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_record);

      insert_cnt++;
      auto index_vec = catalog_->GetTableIndexes(table_info_->name_);
      for (auto index : index_vec) {
        index->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            *child_rid, exec_ctx_->GetTransaction());
      }
    }
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, insert_cnt);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
