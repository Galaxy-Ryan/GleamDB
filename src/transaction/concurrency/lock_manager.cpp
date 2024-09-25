/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

#include <algorithm>
// #include <utility>

/**
 * @description: 申请行级共享锁,需要判断间隙锁逻辑
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    // auto lock_data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_data_id_rec = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto lock_mode = LockMode::SHARED;

    std::unique_lock<std::mutex> lock_txn(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id_rec) == lock_table_.end() || lock_table_[lock_data_id_rec] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id_rec] = lock_request_que;
    }
    auto queue_rec = lock_table_[lock_data_id_rec];
    lock_txn.unlock();

    // add que->latch_
    std::unique_lock<std::mutex> lck_rec(queue_rec->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue_rec->request_queue_.begin(), queue_rec->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });
    if (it != queue_rec->request_queue_.end()) {
        if (queue_rec->group_mode_ == GroupMode::X) {
            bool is_other_write = false;
            for (const auto &request: queue_rec->request_queue_) {
                if (request->txn_id_ != txn->get_transaction_id() && request->granted_ &&
                    request->lock_mode_ == LockMode::EXLUCSIVE) {
                    is_other_write = true;
                    break;
                }
            }
            if (is_other_write) {
                (*it)->granted_ = false;
                wait_die(*queue_rec, *(*it), txn);
                queue_rec->cv_.wait(lck_rec, [&]() { return (*it)->granted_; });
                queue_rec->group_mode_ = queue_rec->group_mode_ == GroupMode::X ? queue_rec->group_mode_ : GroupMode::S;
                return true;
            } else {
                (*it)->granted_ = true;
                return true;
            }
        } else if (queue_rec->group_mode_ == GroupMode::S) {
            (*it)->granted_ = true;
            return true;
        }
    }
    auto request_new = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    queue_rec->request_queue_.push_back(request_new);
    if (queue_rec->group_mode_ != GroupMode::X) {
        queue_rec->group_mode_ = GroupMode::S;
        request_new->granted_ = true;
    } else {
        request_new->granted_ = false;
        wait_die(*queue_rec, *request_new, txn);
        // Wait for the lock to be granted
        queue_rec->cv_.wait(lck_rec, [&]() { return request_new->granted_; });
    }

    txn->get_lock_set()->emplace(lock_data_id_rec);
    return true;
}

/**
 * @description: 申请行级排他锁
 *
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    // we don't need this function on gap lock
    return true;
}

/**
 * @description: 申请行级排他锁,需要判断间隙锁逻辑
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */

bool LockManager::lock_exclusive_on_record_gap(Transaction *txn, const Rid &rid, int tab_fd, char *key,
                                               IndexMeta &indexMeta,IxIndexHandle* ih) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    auto lock_data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_data_id_rec = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto lock_mode = LockMode::EXLUCSIVE;

    std::unique_lock<std::mutex> lock_txn(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id_rec) == lock_table_.end() || lock_table_[lock_data_id_rec] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id_rec] = lock_request_que;
    }
    if (lock_table_.find(lock_data_id_table) == lock_table_.end() || lock_table_[lock_data_id_table] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id_table] = lock_request_que;
    }
    auto queue_rec = lock_table_[lock_data_id_rec];
    auto queue_table = lock_table_[lock_data_id_table];
    lock_txn.unlock();

    // add que->latch_
    std::unique_lock<std::mutex> lck_rec(queue_rec->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue_rec->request_queue_.begin(), queue_rec->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });

    auto insert_key = std::make_shared<InsertKey>();
    insert_key->txn_id = txn->get_transaction_id();
    insert_key->indexMeta = indexMeta;
    insert_key->insert_key = key;
    insert_key->rid = rid;
    insert_key->ih_ = ih;
    queue_table->insert_keys_.emplace_back(insert_key);

    std::unique_lock<std::mutex> lck_table(queue_table->latch_);
    for (auto &request: queue_table->request_queue_) {
        for (auto &gap: request->gaps) {
            if (gap->lower_key_ != nullptr && gap->upper_key_ != nullptr) {
                if (insert_key->txn_id != request->txn_id_ && ih->get_fd() == gap->ih_->get_fd() &&
                    ix_compare(key, gap->lower_key_, indexMeta.cols) > 0 &&
                    ix_compare(gap->upper_key_, key, indexMeta.cols) > 0) {
                    insert_key->is_waited = true;
                    break;
                }
            }
        }
        if (insert_key->is_waited) {
            break;
        }
    }
    lck_table.unlock();
    if (it != queue_rec->request_queue_.end()) {
        if ((*it)->lock_mode_ == LockMode::SHARED) {
            if (queue_rec->upgrading_ != INVALID_TXN_ID) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            queue_rec->upgrading_ = txn->get_transaction_id();
            bool is_other_hold = false;

            for (const auto &request: queue_rec->request_queue_) {
                if (request->txn_id_ != txn->get_transaction_id() && request->granted_) {
                    is_other_hold = true;
                    break;
                }
            }
            if (is_other_hold || insert_key->is_waited) {
                (*it)->granted_ = false;
                (*it)->lock_mode_ = LockMode::EXLUCSIVE;
                wait_die(*queue_rec, *(*it), txn);
                if (queue_table != nullptr && queue_table->group_mode_ == GroupMode::GAP) {
                    wait_die(*queue_table, *(*it), txn);
                }
                // Wait for the lock to be granted
                queue_rec->cv_.wait(lck_rec, [&]() { return (*it)->granted_; });
                queue_rec->group_mode_ = GroupMode::X;
                if (queue_rec->upgrading_ == txn->get_transaction_id()) {
                    queue_rec->upgrading_ = INVALID_TXN_ID;
                }
                return true;
            }
            (*it)->granted_ = true;
            (*it)->lock_mode_ = LockMode::EXLUCSIVE;
            queue_rec->group_mode_ = GroupMode::X;
            if (queue_rec->upgrading_ == txn->get_transaction_id()) {
                queue_rec->upgrading_ = INVALID_TXN_ID;
            }
            return true;
        } else if ((*it)->lock_mode_ == LockMode::EXLUCSIVE) {
            return true;
        }
    }

    auto request_new = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    if (queue_rec->request_queue_.empty()) {
        queue_rec->request_queue_.push_back(request_new);
        if (insert_key->is_waited) {
            request_new->granted_ = false;
            request_new->lock_mode_ = lock_mode;
            if (queue_table != nullptr && queue_table->group_mode_ == GroupMode::GAP) {
                wait_die(*queue_table, *request_new, txn);
            }
            wait_die(*queue_rec, *request_new, txn);
            // Wait for the lock to be granted
            queue_rec->cv_.wait(lck_rec, [&]() { return request_new->granted_; });
            queue_rec->group_mode_ = GroupMode::X;
        } else {
            request_new->granted_ = true;
            queue_rec->group_mode_ = GroupMode::X;
        }
        txn->get_lock_set()->emplace(lock_data_id_rec);
        txn->get_lock_set()->emplace(lock_data_id_table);
        return true;
    }

    queue_rec->request_queue_.push_back(request_new);
    bool is_compatible = true;
    for (const auto &request: queue_rec->request_queue_) {
        if (request->txn_id_ != txn->get_transaction_id() && request->granted_) {
            is_compatible = false;
            break;
        }
    }

    if (is_compatible && !insert_key->is_waited) {
        request_new->granted_ = true;
        queue_rec->group_mode_ = GroupMode::X;
    } else {
        wait_die(*queue_rec, *request_new, txn);
        if (queue_table != nullptr && queue_table->group_mode_ == GroupMode::GAP) {
            wait_die(*queue_table, *request_new, txn);
        }
        // Wait for the lock to be granted
        queue_rec->cv_.wait(lck_rec, [&]() { return request_new->granted_; });
        queue_rec->group_mode_ = GroupMode::X;
    }
    txn->get_lock_set()->emplace(lock_data_id_rec);
    txn->get_lock_set()->emplace(lock_data_id_table);
    return true;
}

/**
 * @description: 申请间隙锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */

bool
LockManager::lock_GAP_on_table(Transaction *txn, int tab_fd, char *lower_key, char *upper_key, IndexMeta &index_meta,
                               std::vector<Condition> conds, IxIndexHandle *ih) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_mode = LockMode::GAP;
    auto gap = std::make_shared<Gap>();
    gap->index_meta_ = index_meta;
    gap->upper_key_ = upper_key;
    gap->lower_key_ = lower_key;
    gap->conds_ = conds;
    gap->ih_ = ih;

    std::unique_lock<std::mutex> lock(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id) == lock_table_.end() || lock_table_[lock_data_id] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id] = lock_request_que;
    }
    auto queue = lock_table_[lock_data_id];
    lock.unlock();
    // add gap->latch_
    std::unique_lock<std::mutex> lck(gap->latch_);
    std::unique_lock<std::mutex> lck_queue(queue->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });

    if (queue->group_mode_ != GroupMode::TAB_X) {
        queue->group_mode_ = GroupMode::GAP;
    }
    if (it != queue->request_queue_.end()) {
        if (queue->group_mode_ == GroupMode::TAB_X) {
            bool is_other_write = false;
            for (const auto &request: queue->request_queue_) {
                if (request->granted_ && request->txn_id_ != (*it)->txn_id_ &&
                    request->lock_mode_ == LockMode::EXLUCSIVE) {
                    is_other_write = true;
                    break;
                }
            }
            if (is_other_write) {
                //(*it)->granted_ = false;
                (*it)->lock_mode_ = lock_mode;
                gap->conflicted = true;
                (*it)->gaps.push_back(gap);
                wait_die(*queue, *(*it), txn);
                // Wait for the lock to be granted
                queue->cv_.wait(lck, [&]() { return !gap->conflicted; });
                lck_queue.unlock();
                return true;
            } else {
                (*it)->gaps.push_back(gap);
                lck_queue.unlock();
                return true;
            }
        }
        (*it)->lock_mode_ = lock_mode;
        for (auto &insert_key: queue->insert_keys_) {
            if (gap->lower_key_ != nullptr && gap->upper_key_ != nullptr) {
                if (!insert_key->is_waited && insert_key->txn_id != txn->get_transaction_id() &&
                    insert_key->ih_->get_fd() == gap->ih_->get_fd() &&
                    ix_compare(insert_key->insert_key, gap->lower_key_, insert_key->indexMeta.cols) > 0 &&
                    ix_compare(gap->upper_key_, insert_key->insert_key, insert_key->indexMeta.cols) > 0) {
                    gap->conflicted = true;
                    // (*it)->granted_ = false;
                    // Wait-Die strategy for lock
                    (*it)->lock_mode_ = lock_mode;
                    (*it)->gaps.push_back(gap);
                    wait_die(*queue, *(*it), txn);
                    // Wait for the lock to be granted
                    queue->cv_.wait(lck, [&]() { return !gap->conflicted; });
                    lck_queue.unlock();
                    // (*it)->granted_ = true;
                    return true;
                }
            }

        }
        // (*it)->granted_ = true;
        (*it)->lock_mode_ = lock_mode;
        (*it)->gaps.push_back(gap);
        lck_queue.unlock();
        return true;
    }

    // Create a new lock request
    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    request->gaps.push_back(gap);
    if (queue->request_queue_.empty()) {
        queue->request_queue_.push_back(request);
        for (auto &insert_key: queue->insert_keys_) {
            if (gap->lower_key_ != nullptr && gap->upper_key_ != nullptr) {
                if (!insert_key->is_waited && insert_key->txn_id != txn->get_transaction_id() &&
                    insert_key->ih_->get_fd() == gap->ih_->get_fd() &&
                    ix_compare(insert_key->insert_key, gap->lower_key_, insert_key->indexMeta.cols) > 0 &&
                    ix_compare(gap->upper_key_, insert_key->insert_key, insert_key->indexMeta.cols) > 0) {
                    gap->conflicted = true;
                    break;
                }
            }

        }
        lck_queue.unlock();
        if (gap->conflicted) {
            // Wait-Die strategy for lock
            request->lock_mode_ = lock_mode;
            wait_die(*queue, *request, txn);
            // Wait for the lock to be granted
            queue->cv_.wait(lck, [&]() { return !gap->conflicted; });
            // request->granted_ = true;
            queue->group_mode_ = GroupMode::GAP;
            txn->get_lock_set()->emplace(lock_data_id);
            return true;
        }
        // request->granted_ = true;
        queue->group_mode_ = GroupMode::GAP;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }

    queue->request_queue_.push_back(request);
    if (queue->group_mode_ == GroupMode::TAB_X) {
        request->lock_mode_ = lock_mode;
        gap->conflicted = true;
        request->gaps.push_back(gap);
        wait_die(*queue, *request, txn);
        // Wait for the lock to be granted
        queue->cv_.wait(lck, [&]() { return !gap->conflicted; });
        lck_queue.unlock();
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }
    for (const auto &insert_key: queue->insert_keys_) {
        if (gap->lower_key_ != nullptr && gap->upper_key_ != nullptr) {
            if (!insert_key->is_waited && insert_key->txn_id != txn->get_transaction_id() &&
                insert_key->ih_->get_fd() == gap->ih_->get_fd() &&
                ix_compare(insert_key->insert_key, gap->lower_key_, insert_key->indexMeta.cols) > 0 &&
                ix_compare(gap->upper_key_, insert_key->insert_key, insert_key->indexMeta.cols) > 0) {
                gap->conflicted = true;
                // request->granted_ = false;
                break;
            }
        }
    }
    lck_queue.unlock();
    if (gap->conflicted) {
        // Wait-Die strategy for lock
        request->lock_mode_ = lock_mode;
        wait_die(*queue, *request, txn);
        // Wait for the lock to be granted
        queue->cv_.wait(lck, [&]() { return !gap->conflicted; });
    }
    // request->granted_ = true;
    txn->get_lock_set()->emplace(lock_data_id);
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_mode = LockMode::INTENSION_SHARED;

    std::unique_lock<std::mutex> lock_txn(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id) == lock_table_.end() || lock_table_[lock_data_id] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id] = lock_request_que;
    }
    auto queue_table = lock_table_[lock_data_id];
    lock_txn.unlock();

    // add que->latch_
    std::unique_lock<std::mutex> lck_tab(queue_table->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue_table->request_queue_.begin(), queue_table->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });
    if (it != queue_table->request_queue_.end()) {
        if (queue_table->group_mode_ == GroupMode::GAP || queue_table->group_mode_ == GroupMode::IS ||
            queue_table->group_mode_ == GroupMode::IX) {
            // already hold a compatible and higher lock on this lock_data_id
            (*it)->granted_ = true;
            return true;
        } else if (queue_table->group_mode_ == GroupMode::TAB_X) {
            bool is_other_hold = false;
            for (const auto &request: queue_table->request_queue_) {
                if (request->granted_ && request->txn_id_ != (*it)->txn_id_ &&
                    request->lock_mode_ == LockMode::EXLUCSIVE) {
                    is_other_hold = true;
                    break;
                }
            }
            if (is_other_hold) {
                (*it)->granted_ = false;
                wait_die(*queue_table, *(*it), txn);
                // Wait for the lock to be granted
                queue_table->cv_.wait(lck_tab, [&]() { return (*it)->granted_; });
                queue_table->group_mode_ =
                        queue_table->group_mode_ == GroupMode::NO_LOCK ? GroupMode::IS : queue_table->group_mode_;
                return true;
            } else {
                (*it)->granted_ = true;
                return true;
            }
        }
    }
    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    if (queue_table->request_queue_.empty()) {
        request->granted_ = true;
        queue_table->request_queue_.push_back(request);
        queue_table->group_mode_ = GroupMode::IS;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }
    queue_table->request_queue_.push_back(request);
    if (queue_table->group_mode_ == GroupMode::GAP || queue_table->group_mode_ == GroupMode::IS ||
        queue_table->group_mode_ == GroupMode::IX) {
        // already hold a compatible and higher lock on this lock_data_id
        request->granted_ = true;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    } else if (queue_table->group_mode_ == GroupMode::TAB_X) {
        request->granted_ = false;
        wait_die(*queue_table, *request, txn);
        // Wait for the lock to be granted
        queue_table->cv_.wait(lck_tab, [&]() { return request->granted_; });
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请表级意向排他锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_mode = LockMode::INTENSION_EXLUCSIVE;

    std::unique_lock<std::mutex> lock_txn(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id) == lock_table_.end() || lock_table_[lock_data_id] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id] = lock_request_que;
    }
    auto queue_table = lock_table_[lock_data_id];
    lock_txn.unlock();

    // add que->latch_
    std::unique_lock<std::mutex> lck_tab(queue_table->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue_table->request_queue_.begin(), queue_table->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });
    if (it != queue_table->request_queue_.end()) {
        if (queue_table->group_mode_ == GroupMode::GAP || queue_table->group_mode_ == GroupMode::IX) {
            // already hold a compatible or higher lock on this lock_data_id
            (*it)->granted_ = true;
            (*it)->lock_mode_ =
                    (*it)->lock_mode_ == LockMode::INTENSION_SHARED ? LockMode::INTENSION_EXLUCSIVE : (*it)->lock_mode_;
            return true;
        } else if (queue_table->group_mode_ == GroupMode::IS) {
            (*it)->granted_ = true;
            (*it)->lock_mode_ = lock_mode;
            queue_table->group_mode_ = GroupMode::IX;
            return true;
        } else if (queue_table->group_mode_ == GroupMode::TAB_X) {
            bool is_other_hold = false;
            for (const auto &request: queue_table->request_queue_) {
                if (request->granted_ && request->txn_id_ != (*it)->txn_id_ &&
                    request->lock_mode_ == LockMode::EXLUCSIVE) {
                    is_other_hold = true;
                    break;
                }
            }
            if (is_other_hold) {
                (*it)->granted_ = false;
                (*it)->lock_mode_ = (*it)->lock_mode_ == LockMode::GAP ? (*it)->lock_mode_ : lock_mode;
                wait_die(*queue_table, *(*it), txn);
                // Wait for the lock to be granted
                queue_table->cv_.wait(lck_tab, [&]() { return (*it)->granted_; });
                queue_table->group_mode_ =
                        queue_table->group_mode_ == GroupMode::GAP ? queue_table->group_mode_ : ((*it)->lock_mode_ ==
                                                                                                 LockMode::GAP
                                                                                                 ? GroupMode::GAP
                                                                                                 : GroupMode::IX);
                return true;
            } else {
                (*it)->granted_ = true;
                return true;
            }
        }
    }

    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    if (queue_table->request_queue_.empty()) {
        request->granted_ = true;
        queue_table->request_queue_.push_back(request);
        queue_table->group_mode_ = GroupMode::IX;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }
    queue_table->request_queue_.push_back(request);
    if (queue_table->group_mode_ == GroupMode::GAP || queue_table->group_mode_ == GroupMode::IX) {
        request->granted_ = true;
    } else if (queue_table->group_mode_ == GroupMode::IS) {
        request->granted_ = true;
        queue_table->group_mode_ = GroupMode::IX;
    } else if (queue_table->group_mode_ == GroupMode::TAB_X) {
        request->granted_ = false;
        wait_die(*queue_table, *request, txn);
        // Wait for the lock to be granted
        queue_table->cv_.wait(lck_tab, [&]() { return request->granted_; });
    }
    txn->get_lock_set()->emplace(lock_data_id);
    return true;
}

/**
 * @description: 申请表级排他锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_mode = LockMode::EXLUCSIVE;

    std::unique_lock<std::mutex> lock_txn(latch_);

    // Find or create the lock request queue for this lock_data_id
    if (lock_table_.find(lock_data_id) == lock_table_.end() || lock_table_[lock_data_id] == nullptr) {
        auto lock_request_que = std::make_shared<LockRequestQueue>();
        lock_table_[lock_data_id] = lock_request_que;
    }
    auto queue_table = lock_table_[lock_data_id];
    lock_txn.unlock();

    // add que->latch_
    std::unique_lock<std::mutex> lck_tab(queue_table->latch_);
    // Check if the transaction already holds a lock on this lock_data_id
    auto it = std::find_if(queue_table->request_queue_.begin(), queue_table->request_queue_.end(),
                           [txn](const std::shared_ptr<LockRequest> &req) {
                               return req->txn_id_ == txn->get_transaction_id();
                           });
    if (it != queue_table->request_queue_.end()) {
        if (queue_table->upgrading_ != INVALID_TXN_ID) {
            txn->set_state(TransactionState::ABORTED);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
        queue_table->upgrading_ = txn->get_transaction_id();
        auto is_other_hold = false;
        for (const auto &req: queue_table->request_queue_) {
            if (req->granted_ && req->txn_id_ != (*it)->txn_id_) {
                is_other_hold = true;
                break;
            } else if (queue_table->group_mode_ == GroupMode::GAP && req->txn_id_ != (*it)->txn_id_ &&
                       req->lock_mode_ == LockMode::GAP) {
                for (const auto &gap: req->gaps) {
                    if (!gap->conflicted) {
                        is_other_hold = true;
                        break;
                    }
                }
                if (is_other_hold) {
                    break;
                }
            }
        }
        if (is_other_hold) {
            (*it)->granted_ = false;
            (*it)->lock_mode_ = lock_mode;
            wait_die(*queue_table, *(*it), txn);
            // Wait for the lock to be granted
            queue_table->cv_.wait(lck_tab, [&]() { return (*it)->granted_; });
            queue_table->group_mode_ = GroupMode::TAB_X;
            if (queue_table->upgrading_ == txn->get_transaction_id()) {
                queue_table->upgrading_ = INVALID_TXN_ID;
            }
            return true;
        } else {
            (*it)->granted_ = true;
            (*it)->lock_mode_ = lock_mode;
            queue_table->group_mode_ = GroupMode::TAB_X;
            if (queue_table->upgrading_ == txn->get_transaction_id()) {
                queue_table->upgrading_ = INVALID_TXN_ID;
            }
            return true;
        }

    }
    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), lock_mode);
    if (queue_table->request_queue_.empty()) {
        request->granted_ = true;
        queue_table->request_queue_.push_back(request);
        queue_table->group_mode_ = GroupMode::TAB_X;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }
    queue_table->request_queue_.push_back(request);
    if (queue_table->group_mode_ != GroupMode::NO_LOCK) {
        request->granted_ = false;
        wait_die(*queue_table, *request, txn);
        // Wait for the lock to be granted
        queue_table->cv_.wait(lck_tab, [&]() { return request->granted_; });
        queue_table->group_mode_ = GroupMode::TAB_X;
        txn->get_lock_set()->emplace(lock_data_id);
        return true;
    }

    request->granted_ = true;
    queue_table->group_mode_ = GroupMode::TAB_X;
    txn->get_lock_set()->emplace(lock_data_id);
    return true;
}


/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 查找锁请求队列
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) {
        lock.unlock();
        return false; // 如果找不到锁请求队列，返回 false
    }
    lock.unlock();

    // std::unique_lock<std::mutex> lck(it->second->latch_);
    LockRequestQueue &request_queue = *it->second;

    // 查找当前事务的锁请求
    auto req_it = std::find_if(request_queue.request_queue_.begin(), request_queue.request_queue_.end(),
                               [txn](const std::shared_ptr<LockRequest> &req) {
                                   return req->txn_id_ == txn->get_transaction_id();
                               });

    if (req_it == request_queue.request_queue_.end()) {
        return false; // 如果找不到当前事务的锁请求，返回 false
    }

    // 移除锁请求
    if ((request_queue.group_mode_ == GroupMode::GAP || request_queue.group_mode_ == GroupMode::TAB_X) &&
        lock_data_id.type_ == LockDataType::TABLE) {
        if (!request_queue.insert_keys_.empty()) {
            request_queue.insert_keys_.erase(
                    remove_if(request_queue.insert_keys_.begin(), request_queue.insert_keys_.end(),
                              [txn](const std::shared_ptr<InsertKey> &insert_key) {
                                  return insert_key->txn_id == txn->get_transaction_id();
                              }),
                    request_queue.insert_keys_.end());
        }
        request_queue.request_queue_.erase(req_it);
        auto group_lock = GroupMode::NO_LOCK;
        for (auto &req: request_queue.request_queue_) {
            if (req->lock_mode_ == LockMode::GAP && group_lock != GroupMode::TAB_X) {
                for (const auto &gap: req->gaps) {
                    if (!gap->conflicted) {
                        group_lock = GroupMode::GAP;
                        break;
                    }
                }
            }
            if (req->granted_) {
                switch (req->lock_mode_) {
                    case LockMode::EXLUCSIVE:
                        group_lock = GroupMode::TAB_X;
                        break;
                    case LockMode::INTENSION_EXLUCSIVE:
                        if (group_lock != GroupMode::TAB_X && group_lock != GroupMode::GAP) {
                            group_lock = GroupMode::IX;
                        }
                        break;
                    case LockMode::INTENSION_SHARED:
                        if (group_lock == GroupMode::NO_LOCK) {
                            group_lock = GroupMode::IS;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        request_queue.group_mode_ = group_lock;

    } else if (request_queue.group_mode_ == GroupMode::X && lock_data_id.type_ == LockDataType::RECORD) {
        std::unique_lock<std::mutex> lck_table(latch_);
        auto &queue_table = lock_table_[LockDataId(lock_data_id.fd_, LockDataType::TABLE)];
        lck_table.unlock();
        if (queue_table != nullptr && !queue_table->insert_keys_.empty()) {
            queue_table->insert_keys_.erase(
                    remove_if(queue_table->insert_keys_.begin(), queue_table->insert_keys_.end(),
                              [txn](const std::shared_ptr<InsertKey> &insert_key) {
                                  return insert_key->txn_id == txn->get_transaction_id();
                              }),
                    queue_table->insert_keys_.end());
        }
        request_queue.request_queue_.erase(req_it);
        auto group_lock = GroupMode::NO_LOCK;
        for (auto &req: request_queue.request_queue_) {
            if (req->granted_) {
                switch (req->lock_mode_) {
                    case LockMode::EXLUCSIVE:
                        group_lock = GroupMode::X;
                        break;
                    case LockMode::SHARED:
                        if (group_lock == GroupMode::NO_LOCK) {
                            group_lock = GroupMode::S;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        request_queue.group_mode_ = group_lock;
    } else {
        request_queue.request_queue_.erase(req_it);
        auto group_lock = GroupMode::NO_LOCK;
        if (lock_data_id.type_ == LockDataType::TABLE) {
            for (auto &req: request_queue.request_queue_) {
                if (req->lock_mode_ == LockMode::GAP && group_lock != GroupMode::TAB_X) {
                    for (const auto &gap: req->gaps) {
                        if (!gap->conflicted) {
                            group_lock = GroupMode::GAP;
                            break;
                        }
                    }
                }
                if (req->granted_) {
                    switch (req->lock_mode_) {
                        case LockMode::EXLUCSIVE:
                            group_lock = GroupMode::TAB_X;
                            break;
                        case LockMode::INTENSION_EXLUCSIVE:
                            if (group_lock != GroupMode::TAB_X && group_lock != GroupMode::GAP) {
                                group_lock = GroupMode::IX;
                            }
                            break;
                        case LockMode::INTENSION_SHARED:
                            if (group_lock == GroupMode::NO_LOCK) {
                                group_lock = GroupMode::IS;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        } else if (lock_data_id.type_ == LockDataType::RECORD) {
            for (auto &req: request_queue.request_queue_) {
                if (req->granted_) {
                    switch (req->lock_mode_) {
                        case LockMode::EXLUCSIVE:
                            group_lock = GroupMode::X;
                            break;
                        case LockMode::SHARED:
                            if (group_lock == GroupMode::NO_LOCK) {
                                group_lock = GroupMode::S;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        request_queue.group_mode_ = group_lock;
    }

    if (request_queue.upgrading_ == txn->get_transaction_id()) {
        request_queue.upgrading_ = INVALID_TXN_ID;
    }

    // 如果请求队列为空，移除锁数据标识符
    if (request_queue.request_queue_.empty() && request_queue.insert_keys_.empty()) {
        std::unique_lock<std::mutex> lock_(latch_);
        request_queue.group_mode_ = GroupMode::NO_LOCK;
        lock_table_.erase(it);
        lock_.unlock();
    } else {
        // 重新授予锁,只针对部分情况
        grant_lock(request_queue, lock_data_id.type_);
        // 特殊情况,授予行级排它锁
        if (lock_data_id.type_ == LockDataType::TABLE) {
            for (auto &key: request_queue.insert_keys_) {
                if (key->is_waited) {
                    bool is_ganted = true;
                    auto txn_id = key->txn_id;
                    for (auto &request: request_queue.request_queue_) {
                        for (auto &gap: request->gaps) {
                            if (gap->lower_key_ != nullptr && gap->upper_key_ != nullptr) {
                                if (key->txn_id != request->txn_id_ && key->ih_->get_fd() == gap->ih_->get_fd() &&
                                    ix_compare(key->insert_key, gap->lower_key_, key->indexMeta.cols) > 0 &&
                                    ix_compare(gap->upper_key_, key->insert_key, key->indexMeta.cols) > 0) {
                                    is_ganted = false;
                                    break;
                                }
                            }
                        }
                    }

                    std::unique_lock<std::mutex> lock_rec(latch_);
                    auto queue_rec = lock_table_[LockDataId(lock_data_id.fd_, key->rid, LockDataType::RECORD)];
                    lock_rec.unlock();
                    auto it = std::find_if(queue_rec->request_queue_.begin(), queue_rec->request_queue_.end(),
                                           [txn_id](const std::shared_ptr<LockRequest> &req) {
                                               return req->txn_id_ == txn_id;
                                           });
                    if (it != queue_rec->request_queue_.end() && (*it)->lock_mode_ == LockMode::EXLUCSIVE) {
                        if (is_ganted) {
                            (*it)->granted_ = true;
                            key->is_waited = false;
                        } else if ((*it)->txn_id_ == request_queue.upgrading_) {
                            (*it)->granted_ = true;
                            key->is_waited = false;
                            request_queue.group_mode_ = GroupMode::X;
                            // request_queue.upgrading_ = INVALID_TXN_ID;
                        }
                        queue_rec->cv_.notify_all();
                    }
                }
            }
        }
        if (lock_data_id.type_ == LockDataType::RECORD) {
            std::unique_lock<std::mutex> lock_tab(latch_);
            auto queue_tab = lock_table_[LockDataId(lock_data_id.fd_, LockDataType::TABLE)];
            lock_tab.unlock();
            if (queue_tab != nullptr && queue_tab->group_mode_ != GroupMode::GAP &&
                queue_tab->group_mode_ != GroupMode::TAB_X) {
                for (auto &request: request_queue.request_queue_) {
                    if (!request->granted_) {
                        if (request_queue.group_mode_ == GroupMode::NO_LOCK &&
                            request->lock_mode_ == LockMode::EXLUCSIVE) {
                            request->granted_ = true;
                            request_queue.group_mode_ = GroupMode::X;
                        } else if (request->txn_id_ == request_queue.upgrading_) {
                            bool is_other_hold = false;
                            for (const auto &req: request_queue.request_queue_) {
                                if (req->txn_id_ != request->txn_id_ && req->granted_) {
                                    is_other_hold = true;
                                    break;
                                }
                            }
                            if(!is_other_hold) {
                                request->granted_ = true;
                                request_queue.group_mode_ = GroupMode::X;
                            }
                            // request_queue.upgrading_ = INVALID_TXN_ID;
                        }
                    }

                }
                request_queue.cv_.notify_all();
            }
        }
    }
    return true;
}

void LockManager::calculate_gaps_on_table(LockDataId lock_data_id) {
    if (lock_data_id.type_ != LockDataType::TABLE) {
        return;
    }

    std::unique_lock<std::mutex> lock(latch_);

    // 查找锁请求队列
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) {
        lock.unlock();
        return; // 如果找不到锁请求队列,直接返回
    }
    lock.unlock();

    LockRequestQueue &request_queue = *it->second;
    if (request_queue.group_mode_ == GroupMode::GAP) {
        for (auto &request: request_queue.request_queue_) {
            for (auto &gap: request->gaps) {
                auto lower_key = new char[gap->index_meta_.col_tot_len];
                auto upper_key = new char[gap->index_meta_.col_tot_len];
                auto bound = get_bound(lower_key, upper_key, gap->index_meta_, gap->conds_, gap->ih_);
                get_gap_lock_key(lower_key, upper_key, bound, gap->index_meta_, gap->conds_, gap->ih_);
                gap->upper_key_ = upper_key;
                gap->lower_key_ = lower_key;
            }
        }
    }
}