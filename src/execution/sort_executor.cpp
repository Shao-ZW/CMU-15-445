#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <utility>
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    sorted_tuples_.emplace_back(child_tuple, child_rid);
  }

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
            [&](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) {
              auto &order_bys = plan_->order_bys_;
              for (auto &[type, expr] : order_bys) {
                auto val_a = expr->Evaluate(&a.first, child_executor_->GetOutputSchema());
                auto val_b = expr->Evaluate(&b.first, child_executor_->GetOutputSchema());
                if (type == OrderByType::DESC) {
                  if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
                    return true;
                  }
                  if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
                    return false;
                  }
                } else {
                  if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
                    return true;
                  }
                  if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
                    return false;
                  }
                }
              }

              return true;
            });
  iter_ = sorted_tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_tuples_.cend()) {
    return false;
  }

  *tuple = iter_->first;
  *rid = iter_->second;
  ++iter_;
  return true;
}

}  // namespace bustub
