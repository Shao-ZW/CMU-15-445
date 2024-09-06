//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode, LockObject::TABLE)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  if (lock_request_queue->granted_queue_.empty() && lock_request_queue->request_queue_.empty()) {
    lock_request->granted_ = true;
    lock_request_queue->granted_queue_.push_back(lock_request);
    UpdateTransactionTableInfo(txn, oid, lock_mode, 1);
    return true;
  }

  auto [already_lock, is_upgrade] = TryUpgradeLock(lock_request_queue, txn, lock_mode, LockObject::TABLE);

  if (already_lock) {
    return true;
  }
  if (is_upgrade) {
    lock_request_queue->request_queue_.push_front(lock_request);
  } else {
    lock_request_queue->request_queue_.push_back(lock_request);
  }

  do {
    if (TryGrantLocks(lock_request_queue)) {
      lock_request_queue->cv_.notify_all();
      if (lock_request->granted_) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        UpdateTransactionTableInfo(txn, oid, lock_mode, 1);
        return true;
      }
    }

    lock_request_queue->cv_.wait(lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->granted_queue_.remove(lock_request);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }

    if (lock_request->granted_) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      UpdateTransactionTableInfo(txn, oid, lock_mode, 1);
      return true;
    }
  } while (true);

  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!CanTxnFreeTableLock(txn, oid)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  table_lock_map_latch_.lock();
  lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  LockMode lock_mode;
  lock_request_queue->granted_queue_.remove_if([txn, &lock_mode](std::shared_ptr<LockRequest> &request) {
    if (txn->GetTransactionId() == request->txn_id_) {
      lock_mode = request->lock_mode_;
      return true;
    }
    return false;
  });
  UpdateTransactionTableInfo(txn, oid, lock_mode, 0);
  UpdateTransactionState(txn, lock_mode);
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode, LockObject::ROW)) {
    return false;
  }

  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  if (lock_request_queue->granted_queue_.empty()) {
    lock_request->granted_ = true;
    lock_request_queue->granted_queue_.push_back(lock_request);
    UpdateTransactionRowInfo(txn, oid, rid, lock_mode, 1);
    return true;
  }

  auto [already_lock, is_upgrade] = TryUpgradeLock(lock_request_queue, txn, lock_mode, LockObject::ROW);

  if (already_lock) {
    return true;
  }

  if (is_upgrade) {
    lock_request_queue->request_queue_.push_front(lock_request);
  } else {
    lock_request_queue->request_queue_.push_back(lock_request);
  }

  do {
    if (TryGrantLocks(lock_request_queue)) {
      lock_request_queue->cv_.notify_all();
      if (lock_request->granted_) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        UpdateTransactionRowInfo(txn, oid, rid, lock_mode, 1);
        return true;
      }
    }

    lock_request_queue->cv_.wait(lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->granted_queue_.remove(lock_request);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }

    if (lock_request->granted_) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      UpdateTransactionRowInfo(txn, oid, rid, lock_mode, 1);
      return true;
    }
  } while (true);

  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  if (!CanTxnFreeRowLock(txn, oid, rid)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  row_lock_map_latch_.lock();
  lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  LockMode lock_mode;
  lock_request_queue->granted_queue_.remove_if([txn, &lock_mode](std::shared_ptr<LockRequest> &request) {
    if (txn->GetTransactionId() == request->txn_id_) {
      lock_mode = request->lock_mode_;
      return true;
    }
    return false;
  });
  UpdateTransactionRowInfo(txn, oid, rid, lock_mode, 0);
  if (!force) {
    UpdateTransactionState(txn, lock_mode);
  }
  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::UpdateTransactionTableInfo(Transaction *txn, const table_oid_t &oid, LockMode lock_mode, int type) {
  if (type == 0) {
    if (lock_mode == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->erase(oid);
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->erase(oid);
    } else if (lock_mode == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    }
  } else if (type == 1) {
    if (lock_mode == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    }
  }
}

void LockManager::UpdateTransactionRowInfo(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode lock_mode,
                                           int type) {
  if (type == 0) {
    if (lock_mode == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
    }
  } else if (type == 1) {
    if (lock_mode == LockMode::SHARED) {
      if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->insert({oid, {}});
      }
      txn->GetSharedRowLockSet()->find(oid)->second.insert(rid);
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->insert({oid, {}});
      }
      txn->GetExclusiveRowLockSet()->find(oid)->second.insert(rid);
    }
  }
}

void LockManager::UpdateTransactionState(Transaction *txn, LockMode lock_mode) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}

auto LockManager::TryUpgradeLock(std::shared_ptr<LockRequestQueue> &lock_request_queue, Transaction *txn,
                                 LockMode lock_mode, LockObject object) -> std::pair<bool, bool> {
  for (auto it = lock_request_queue->granted_queue_.begin(); it != lock_request_queue->granted_queue_.end(); ++it) {
    auto &request = *it;
    if (txn->GetTransactionId() == request->txn_id_) {
      if (request->lock_mode_ == lock_mode) {
        return {true, false};
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return {false, false};
      }

      if (CanLockUpgrade(txn, request->lock_mode_, lock_mode)) {
        if (object == LockObject::TABLE) {
          UpdateTransactionTableInfo(txn, (*it)->oid_, (*it)->lock_mode_, 0);
        } else if (object == LockObject::ROW) {
          UpdateTransactionRowInfo(txn, (*it)->oid_, (*it)->rid_, (*it)->lock_mode_, 0);
        }
        lock_request_queue->upgrading_ = txn->GetTransactionId();
        lock_request_queue->granted_queue_.erase(it);
        return {false, true};
      }
    }
  }

  return {false, false};
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  if (l1 == LockMode::INTENTION_SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
           l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (l1 == LockMode::INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
  }

  if (l1 == LockMode::SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
  }

  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED;
  }

  if (l1 == LockMode::EXCLUSIVE) {
    return false;
  }

  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode, LockObject object) -> bool {
  if (object == LockObject::ROW) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      return false;
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (txn->GetState() == TransactionState::GROWING) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }

    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  return true;
}

auto LockManager::TryGrantLocks(std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  bool is_grant = false;
  auto it = lock_request_queue->request_queue_.begin();

  while (it != lock_request_queue->request_queue_.end()) {
    bool grant_flag = true;
    for (auto &granted_request : lock_request_queue->granted_queue_) {
      if (!AreLocksCompatible((*it)->lock_mode_, granted_request->lock_mode_)) {
        grant_flag = false;
        break;
      }
    }

    if (grant_flag) {
      (*it)->granted_ = true;
      is_grant = true;
      lock_request_queue->granted_queue_.push_back(*it);
      ++it;
      lock_request_queue->request_queue_.pop_front();
    } else {
      break;
    }
  }

  return is_grant;
}

auto LockManager::CanLockUpgrade(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    if (!(requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    return true;
  }

  if (curr_lock_mode == LockMode::SHARED) {
    if (!(requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    return true;
  }

  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (!(requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    return true;
  }

  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (!(requested_lock_mode == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    return true;
  }

  if (curr_lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }

  return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }

  return true;
}

auto LockManager::CanTxnFreeTableLock(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!(txn->IsTableSharedLocked(oid) || txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  if (!((txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end() ||
         txn->GetSharedRowLockSet()->find(oid)->second.empty()) &&
        (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end() ||
         txn->GetExclusiveRowLockSet()->find(oid)->second.empty()))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  return true;
}

auto LockManager::CanTxnFreeRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  if (!(txn->IsRowSharedLocked(oid, rid) || txn->IsRowExclusiveLocked(oid, rid))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &waits_list = waits_for_[t1];
  auto it = std::lower_bound(waits_list.begin(), waits_list.end(), t2);
  if (it == waits_list.end() || *it != t2) {
    waits_list.insert(it, t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &waits_list = waits_for_[t1];
  auto it = std::lower_bound(waits_list.begin(), waits_list.end(), t2);
  if (it != waits_list.end() && *it == t2) {
    waits_list.erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_map<txn_id_t, int> state;
  std::vector<txn_id_t> path;

  std::function<bool(txn_id_t)> dfs = [&](txn_id_t tid) {
    state[tid] = 1;
    path.push_back(tid);

    for (auto wait_tid : waits_for_[tid]) {
      if (state.find(wait_tid) != state.end()) {
        if (state[wait_tid] == 0) {
          if (dfs(wait_tid)) {
            return true;
          }
        } else if (state[wait_tid] == 1) {
          auto cycle_start = std::find(path.begin(), path.end(), wait_tid);
          *txn_id = *max_element(cycle_start, path.end());
          return true;
        }
      }
    }

    state[tid] = 2;
    path.pop_back();
    return false;
  };

  *txn_id = INVALID_TXN_ID;
  std::vector<txn_id_t> possible_txn_id;
  possible_txn_id.reserve(waits_for_.size());
  for (auto &[tid, waits_list] : waits_for_) {
    state[tid] = 0;
    possible_txn_id.push_back(tid);
  }

  std::sort(possible_txn_id.begin(), possible_txn_id.end());
  for (auto tid : possible_txn_id) {
    if (state[tid] == 0) {
      path.clear();
      if (dfs(tid)) {
        return true;
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &[node, adjacent_nodes] : waits_for_) {
    edges.reserve(adjacent_nodes.size());
    for (auto &adjacent_node : adjacent_nodes) {
      edges.emplace_back(node, adjacent_node);
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);

    table_lock_map_latch_.lock();
    for (auto &[tid, lock_request_queue] : table_lock_map_) {
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
      for (auto &request : lock_request_queue->request_queue_) {
        txn_table_map_[request->txn_id_] = request->oid_;
        for (auto &granted : lock_request_queue->granted_queue_) {
          if (!AreLocksCompatible(request->lock_mode_, granted->lock_mode_)) {
            AddEdge(request->txn_id_, granted->txn_id_);
          }
        }
      }
    }
    table_lock_map_latch_.unlock();

    row_lock_map_latch_.lock();
    for (auto &[rid, lock_request_queue] : row_lock_map_) {
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
      for (auto &request : lock_request_queue->request_queue_) {
        txn_row_map_[request->txn_id_] = request->rid_;
        for (auto &granted : lock_request_queue->granted_queue_) {
          if (!AreLocksCompatible(request->lock_mode_, granted->lock_mode_)) {
            AddEdge(request->txn_id_, granted->txn_id_);
          }
        }
      }
    }
    row_lock_map_latch_.unlock();

    txn_id_t max_tid;
    while (HasCycle(&max_tid)) {
      auto txn = txn_manager_->GetTransaction(max_tid);
      txn->SetState(TransactionState::ABORTED);
      waits_for_.erase(max_tid);
      for (auto &[tid, waits_list] : waits_for_) {
        RemoveEdge(max_tid, tid);
      }

      if (txn_table_map_.count(max_tid) > 0) {
        auto &oid = txn_table_map_[max_tid];
        table_lock_map_[oid]->latch_.lock();
        table_lock_map_[oid]->cv_.notify_all();
        table_lock_map_[oid]->latch_.unlock();
      }

      if (txn_row_map_.count(max_tid) > 0) {
        auto &rid = txn_row_map_[max_tid];
        row_lock_map_[rid]->latch_.lock();
        row_lock_map_[rid]->cv_.notify_all();
        row_lock_map_[rid]->latch_.unlock();
      }
    }

    waits_for_.clear();
    txn_table_map_.clear();
    txn_row_map_.clear();
  }
}

}  // namespace bustub
