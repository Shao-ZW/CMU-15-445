#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [&](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) {
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
    return false;
  };

  std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, decltype(cmp)> heap(cmp);

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    heap.push({child_tuple, child_rid});
    while (heap.size() > plan_->n_) {
      heap.pop();
    }
  }

  topn_.clear();
  topn_.reserve(plan_->n_);
  for (int i = plan_->n_ - 1; i >= 0 && !heap.empty(); --i) {
    topn_.push_back(heap.top());
    heap.pop();
  }
  std::reverse(topn_.begin(), topn_.end());
  iter_ = topn_.cbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == topn_.cend()) {
    return false;
  }

  *tuple = iter_->first;
  *rid = iter_->second;
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // not used
  return plan_->n_;
};

}  // namespace bustub
