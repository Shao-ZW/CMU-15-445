//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_new_loop_ = true;
  is_left_unjoin_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;

  while (true) {
    if (is_new_loop_) {
      is_new_loop_ = false;
      is_left_unjoin_ = true;
      bool left_status = left_executor_->Next(&left_tuple_, rid);
      if (!left_status) {
        return false;
      }
      right_executor_->Init();
    }

    bool right_status = right_executor_->Next(&right_tuple, rid);

    if (!right_status) {
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

    auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                  right_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      is_left_unjoin_ = false;
      break;
    }
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
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
    }
  }

  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
