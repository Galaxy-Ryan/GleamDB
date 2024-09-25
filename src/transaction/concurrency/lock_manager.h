/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <mutex>
#include <condition_variable>
#include "transaction/transaction.h"
#include "index/ix_index_handle.h"
#include "common/common.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX","GAP"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁 */
    enum class LockMode { SHARED, EXLUCSIVE, GAP,INTENSION_SHARED,INTENSION_EXLUCSIVE };

    enum class GroupMode {NO_LOCK,S,X,GAP,TAB_X,IS,IX};

    /*用于表示索引键间隙*/
    class Gap {
    public:
        char* lower_key_{nullptr};
        char* upper_key_{nullptr};
        IndexMeta index_meta_;
        std::vector<Condition> conds_;
        IxIndexHandle* ih_;
        bool conflicted{false};
        std::mutex latch_;
    };

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
            : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;   // 申请加锁的事务ID
        LockMode lock_mode_;    // 事务申请加锁的类型
        bool granted_;          // 该事务是否已经被赋予锁
        std::list<std::shared_ptr<Gap>> gaps;
    };

    class InsertKey {
    public:
        char* insert_key;
        IndexMeta indexMeta;
        txn_id_t txn_id;
        Rid rid;
        IxIndexHandle* ih_;
        bool is_waited{false};
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::list<std::shared_ptr<LockRequest>> request_queue_;  // 加锁队列
        std::condition_variable cv_;            // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        /** coordination */
        std::mutex latch_;
        /** txn_id of an upgrading transaction (if any) */
        txn_id_t upgrading_{INVALID_TXN_ID};
        GroupMode group_mode_{GroupMode::NO_LOCK};
        std::list<std::shared_ptr<InsertKey>> insert_keys_;
    };

public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);
    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);
    bool lock_exclusive_on_record_gap(Transaction *txn, const Rid &rid, int tab_fd,char* key,IndexMeta &indexMeta,IxIndexHandle* ih);
    bool lock_GAP_on_table(Transaction* txn, int tab_fd,char* lower_key,char* upper_key, IndexMeta& index_meta,std::vector<Condition> conds,IxIndexHandle *ih);
    bool lock_IS_on_table(Transaction* txn, int tab_fd);
    bool lock_IX_on_table(Transaction* txn, int tab_fd);
    bool lock_exclusive_on_table(Transaction *txn, int tab_fd);
    bool unlock(Transaction* txn, LockDataId lock_data_id);
    void calculate_gaps_on_table(LockDataId lock_data_id);


private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, std::shared_ptr<LockRequestQueue>>lock_table_;   // 全局锁表
    // void wait_die(LockRequestQueue& request_queue, LockRequest& request, Transaction* txn);
    // void grant_lock(LockRequestQueue& request_queue,LockDataType type);

    inline std::pair<Iid, Iid> get_bound(char* lower_key, char* upper_key,IndexMeta index_meta_,std::vector<Condition> conds_,IxIndexHandle* ih_) {
        if (conds_.empty()) {
            int offset = 0;
            for (auto& col: index_meta_.cols) {
                Value value;
                value.generate_max(col.type, col.len);
                memcpy(upper_key + offset, value.raw->data, col.len);
            }

            for (auto& col: index_meta_.cols) {
                Value value;
                value.generate_min(col.type, col.len);
                memcpy(lower_key + offset, value.raw->data, col.len);
            }

            return std::make_pair(ih_->leaf_begin(), ih_->leaf_end());
        }

        Iid lower_bound{};
        Iid upper_bound{};
        int match_lower = 0;
        int match_upper = 0;
        CompOp lower_comp = OP_EQ;
        CompOp upper_comp = OP_EQ;
        std::vector<Value> lower_values;
        std::vector<Value> upper_values;

        for (auto& index_col: index_meta_.cols) {
            for (auto& cond: conds_) {
                if (index_col.name == cond.lhs_col.col_name) {
                    if (cond.op == OP_EQ) {
                        lower_values.push_back(cond.rhs_val);
                        upper_values.push_back(cond.rhs_val);
                        match_lower++; match_upper++;
                    } else if (cond.op == OP_GT || cond.op == OP_GE) {
                        lower_values.push_back(cond.rhs_val);
                        match_lower++; lower_comp = cond.op;
                    } else if (cond.op == OP_LT || cond.op == OP_LE) {
                        upper_values.push_back(cond.rhs_val);
                        match_upper++; upper_comp = cond.op;
                    }
                }
            }
        }

        assert( match_lower == match_upper ||
                match_upper - 1 == match_lower ||
                match_lower - 1 == match_upper );

        for (int i = match_lower; i < index_meta_.col_num; i++) {
            Value value;
            ColMeta meta = index_meta_.cols[i];
            if (lower_comp == OP_GT) {
                value.generate_max(meta.type, meta.len);
            } else {
                value.generate_min(meta.type, meta.len);
            }
            lower_values.push_back(value);
        }

        for (int i = match_upper; i < index_meta_.col_num; i++) {
            Value value;
            ColMeta meta = index_meta_.cols[i];
            if (upper_comp == OP_LT) {
                value.generate_min(meta.type, meta.len);
            } else {
                value.generate_max(meta.type, meta.len);
            }
            upper_values.push_back(value);
        }

        int offset = 0;
        for (int i = 0; i < index_meta_.col_num; i++) {
            memcpy(lower_key + offset, lower_values[i].raw->data, index_meta_.cols[i].len);
            offset += index_meta_.cols[i].len;
        }

        offset = 0;
        for (int i = 0; i < index_meta_.col_num; i++) {
            memcpy(upper_key + offset, upper_values[i].raw->data, index_meta_.cols[i].len);
            offset += index_meta_.cols[i].len;
        }

        if (upper_comp == OP_LT) {
            upper_bound = ih_->lower_bound(upper_key);
        } else {
            upper_bound = ih_->upper_bound(upper_key);
        }

        if (lower_comp == OP_GT) {
            lower_bound = ih_->upper_bound(lower_key);
        } else {
            lower_bound = ih_->lower_bound(lower_key);
        }

        return std::make_pair(lower_bound, upper_bound);
    }

    void inline get_gap_lock_key(char* lower_key, char* upper_key, std::pair<Iid, Iid> bound,IndexMeta index_meta_,std::vector<Condition> conds_,IxIndexHandle* ih_) {
        char* lower_bound = new char [index_meta_.col_tot_len];
        char* upper_bound = new char [index_meta_.col_tot_len];

        Iid lower_iid = bound.first;
        Iid upper_iid = bound.second;
        ih_->get_key(lower_iid, lower_bound);
        ih_->get_key(upper_iid, upper_bound);

        int lower_comp = ix_compare(lower_key, lower_bound, index_meta_.cols);

        if (lower_comp < 0) {
            if (ih_->minus_one(lower_iid)) {
                ih_->get_key(lower_iid, lower_bound);
                memcpy(lower_key, lower_bound, index_meta_.col_tot_len);
            }
        } else {
            assert(lower_comp == 0);
        }

        memcpy(upper_key, upper_bound, index_meta_.col_tot_len);
    }

    inline void wait_die(LockRequestQueue &request_queue, LockRequest &request, Transaction *txn) {
        txn_id_t txn_id = request.txn_id_;
        auto it = request_queue.request_queue_.begin();

        while (it != request_queue.request_queue_.end()) {
            if (txn->get_transaction_id() > (*it)->txn_id_) {
                // Current transaction has lower priority, abort it (die)
                txn->set_state(TransactionState::ABORTED);
                if (request_queue.upgrading_ == txn->get_transaction_id()) {
                    request_queue.upgrading_ = INVALID_TXN_ID;
                }
                request_queue.request_queue_.erase(
                        remove_if(request_queue.request_queue_.begin(), request_queue.request_queue_.end(),
                                  [txn_id](const std::shared_ptr<LockRequest> &req) { return req->txn_id_ == txn_id; }),
                        request_queue.request_queue_.end());
                if (!request_queue.insert_keys_.empty()) {
                    request_queue.insert_keys_.erase(
                            remove_if(request_queue.insert_keys_.begin(), request_queue.insert_keys_.end(),
                                      [txn_id](const std::shared_ptr<InsertKey> &insert_key) {
                                          return insert_key->txn_id == txn_id;
                                      }),
                            request_queue.insert_keys_.end());
                }
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
            ++it;
        }
    }

    inline void grant_lock(LockRequestQueue &request_queue, LockDataType type) {
        for (auto &request: request_queue.request_queue_) {
            bool is_granted = false;
            if (!request->granted_) {
                if (type == LockDataType::TABLE) {
                    switch (request->lock_mode_) {
                        case LockMode::EXLUCSIVE:
                            if (request_queue.group_mode_ == GroupMode::NO_LOCK) {
                                request->granted_ = true;
                                is_granted = true;
                                request_queue.group_mode_ = GroupMode::TAB_X;
                            }
                            break;
                        case LockMode::INTENSION_SHARED:
                            if (request_queue.group_mode_ != GroupMode::TAB_X) {
                                request->granted_ = true;
                                is_granted = true;
                                if (request_queue.group_mode_ == GroupMode::NO_LOCK) {
                                    request_queue.group_mode_ = GroupMode::IS;
                                }
                            }
                            break;
                        case LockMode::INTENSION_EXLUCSIVE:
                            if (request_queue.group_mode_ != GroupMode::TAB_X) {
                                request->granted_ = true;
                                is_granted = true;
                                if (request_queue.group_mode_ == GroupMode::NO_LOCK ||
                                    request_queue.group_mode_ == GroupMode::IS) {
                                    request_queue.group_mode_ = GroupMode::IX;
                                }
                            }
                            break;
                        case LockMode::GAP:
                            if(request_queue.group_mode_ != GroupMode::TAB_X) {
                                for (auto &gap: request->gaps) {
                                    bool is_conflict = false;
                                    for (auto &key: request_queue.insert_keys_) {
                                        if (gap->upper_key_ != nullptr && gap->lower_key_ != nullptr) {
                                            if (!key->is_waited && key->txn_id != request->txn_id_ &&
                                                key->indexMeta == gap->index_meta_ &&
                                                ix_compare(key->insert_key, gap->lower_key_, key->indexMeta.cols) > 0 &&
                                                ix_compare(gap->upper_key_, key->insert_key, key->indexMeta.cols) > 0) {
                                                is_conflict = true;
                                                break;
                                            }
                                        }

                                    }
                                    if(!is_conflict && gap->conflicted) {
                                        is_granted = true;
                                        gap->conflicted = is_conflict;
                                    }
                                    // gap->conflicted = is_conflict ? gap->conflicted : is_conflict;
                                }
                            }
                            break;
                        default:
                            break;
                    }
                } else if (type == LockDataType::RECORD) {
                    switch (request->lock_mode_) {
                        case LockMode::SHARED:
                            if (request_queue.group_mode_ != GroupMode::X) {
                                request->granted_ = true;
                                request_queue.group_mode_ = GroupMode::S;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            if(type == LockDataType::TABLE && request->granted_ && request->lock_mode_ == LockMode::GAP) {
                // 重置gap冲突
                for (auto &gap: request->gaps) {
                    bool is_conflict = false;
                    for (auto &key: request_queue.insert_keys_) {
                        if (gap->upper_key_ != nullptr && gap->lower_key_ != nullptr) {
                            if (!key->is_waited && key->txn_id != request->txn_id_ &&
                                key->indexMeta == gap->index_meta_ &&
                                ix_compare(key->insert_key, gap->lower_key_, key->indexMeta.cols) > 0 &&
                                ix_compare(gap->upper_key_, key->insert_key, key->indexMeta.cols) > 0) {
                                is_conflict = true;
                                break;
                            }
                        }

                    }
                    if(!is_conflict && gap->conflicted) {
                        is_granted = true;
                        gap->conflicted = is_conflict;
                    }
                    // gap->conflicted = is_conflict ? gap->conflicted : is_conflict;
                }
                if(is_granted && request_queue.group_mode_ != GroupMode::TAB_X) {
                    request_queue.group_mode_ = GroupMode::GAP;
                }
            }
            if(type == LockDataType::TABLE && !is_granted && request->txn_id_ == request_queue.upgrading_) {
                auto is_other_hold = false;
                for (const auto &req: request_queue.request_queue_) {
                    if (req->granted_ && req->txn_id_ != request->txn_id_) {
                        is_other_hold = true;
                        break;
                    } else if (request_queue.group_mode_ == GroupMode::GAP && req->txn_id_ != request->txn_id_ &&
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
                if(!is_other_hold) {
                    request->granted_ = true;
                    request_queue.group_mode_ = GroupMode::TAB_X;
                }
                // request_queue.upgrading_ = INVALID_TXN_ID;
            }
        }
        request_queue.cv_.notify_all();
    }
};
