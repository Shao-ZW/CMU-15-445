//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple child_tuple;
  RID child_rid;

  aht_.Clear();
  while (child_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }

  // empty
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertEmpty();
  }

  aht_iterator_ = std::make_shared<SimpleAggregationHashTable::Iterator>(aht_.Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.insert(values.end(), aht_iterator_->Key().group_bys_.begin(), aht_iterator_->Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_->Val().aggregates_.begin(), aht_iterator_->Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  ++(*aht_iterator_);

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
