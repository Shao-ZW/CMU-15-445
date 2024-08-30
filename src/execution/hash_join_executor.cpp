//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  is_new_loop_ = true;
  is_left_unjoin_ = true;
  hjht_.clear();

  // build
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    hjht_.insert(
        {MakeRightHashJoinKey(&right_tuple), MakeHashJoinValue(&right_tuple, &right_executor_->GetOutputSchema())});
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (is_new_loop_) {
      is_new_loop_ = false;
      is_left_unjoin_ = true;
      bool left_status = left_executor_->Next(&left_tuple_, rid);
      iter_ = hjht_.cbegin();
      if (!left_status) {
        return false;
      }
    }

    if (iter_ == hjht_.cend()) {
      is_new_loop_ = true;

      if (plan_->GetJoinType() == JoinType::LEFT) {
        if (is_left_unjoin_) {
          break;
        }
        continue;
      }

      if (plan_->GetJoinType() == JoinType::INNER) {
        continue;
      }
    }

    if (MakeLeftHashJoinKey(&left_tuple_) == iter_->first) {
      is_left_unjoin_ = false;
      break;
    }
    ++iter_;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
  }

  if (is_left_unjoin_) {
    assert(plan_->GetJoinType() == JoinType::LEFT);
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    }
  } else {
    values.insert(values.end(), iter_->second.values_.begin(), iter_->second.values_.end());
    ++iter_;
  }

  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
