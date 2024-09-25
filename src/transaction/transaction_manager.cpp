/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
// #include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    // std::unique_lock<std::mutex> lock(latch_);
    txn_id_t txn_id;
//    timestamp_t start_ts;
//    if (txn == nullptr) {
//        txn_id = next_txn_id_++;
//        start_ts = next_timestamp_++;
//        txn = new Transaction(txn_id);
//        txn->set_start_ts(start_ts);
//    } else {
//        txn_id = txn->get_transaction_id();
//    }
//    txn->set_state(TransactionState::GROWING);

    std::unique_lock<std::mutex> lock(latch_);
    if (txn == nullptr) {
        txn_id = next_txn_id_++;
        log_manager->flush_log_to_disk();
        txn_map[txn_id] = new Transaction(txn_id);
        txn = txn_map[txn_id];
        txn->set_start_ts(next_timestamp_++);
    } else {
        txn_id = txn->get_transaction_id();
        txn_map[txn_id] = txn;
    }
    txn->set_state(TransactionState::GROWING);
    lock.unlock();

    auto record = BeginLogRecord(txn->get_transaction_id());
    record.prev_lsn_ = txn->get_prev_lsn();
    auto lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);

    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 提交所有的写操作
    auto write_set = txn->get_write_set();
    if(!write_set->empty()) {
        write_set->clear();
    }
//    while (!write_set->empty()) {
//        WriteRecord record = write_set->front();
//        write_set->pop_front();
//        RmFileHandle* file_handle = sm_manager_->fhs_[record.GetTableName()].get();
//        if(record.record_ != nullptr || record.record_old_ != nullptr){
//            switch (record.GetWriteType()) {
//                case WType::INSERT_TUPLE:
//                    // 插入元组
//                    if(!txn->get_txn_mode()){
//                        file_handle->insert_record(record.GetRecord().data, nullptr);
//                    }
//                    break;
//                case WType::DELETE_TUPLE:
//                    // 删除元组
//                    if(!txn->get_txn_mode()) {
//                        file_handle->delete_record(record.GetRid(), nullptr);
//                    }
//                    break;
//                case WType::UPDATE_TUPLE:
//                    // 更新元组
//                    if(record.GetRecord().allocated_) {
//                        file_handle->update_record(record.GetRid(), record.GetRecord().data, nullptr);
//                    }
//                    break;
//            }
//            // delete record;
//        }
//
//    }

    // 释放所有锁
    for (const auto& lock_data_id : *txn->get_lock_set()) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    // 释放资源
    txn->get_lock_set()->clear();

    if (log_manager != nullptr) {
        auto record = CommitLogRecord(txn->get_transaction_id());
        record.prev_lsn_ = txn->get_prev_lsn();
        auto lsn = log_manager->add_log_to_buffer(&record);
        txn->set_prev_lsn(lsn);
    }

    // 更新事务状态并释放资源
//    std::unique_lock<std::mutex> lock(latch_);
//    txn_map.erase(txn->get_transaction_id());
//    lock.unlock();

    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager, SmManager* sm_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 回滚所有写操作
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        WriteRecord record = write_set->back();
        write_set->pop_back();
        RmFileHandle* file_handle = sm_manager_->fhs_[record.GetTableName()].get();
        auto indexes = sm_manager_->db_.get_table(record.GetTableName()).indexes;
        auto tab_name_ = record.GetTableName();
        auto tab_ = sm_manager_->db_.get_table(tab_name_);
        if(record.record_ != nullptr || record.record_old_ != nullptr) {
            switch (record.GetWriteType()) {
                case WType::INSERT_TUPLE:
                    // 删除插入的元组
                    file_handle->delete_record(record.GetRid(), nullptr);
                    // 删除索引
                    if(!indexes.empty()) {
                        // is_abort_index = true;
                        for (auto &index: indexes) {
                            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                            char *key = new char[index.col_tot_len];
                            for (size_t i = 0; i < index.col_num; ++i) {
                                auto offset = tab_.get_col(index.cols[i].name)->offset;
                                memcpy(key + index.cols[i].offset, record.GetRecord().data + offset, index.cols[i].len);
                            }
                            ih->delete_entry(key, txn);
                            ih->index_aborted_ = true;
                        }
                        for (const auto& lock_data_id : *txn->get_lock_set()) {
                            lock_manager_->calculate_gaps_on_table(lock_data_id);
                        }
                    }

                    break;
                case WType::DELETE_TUPLE:
                    // 恢复删除的元组
                    file_handle->insert_record(record.GetRid(), record.GetRecord().data);
                    // 插入索引
                    if(!indexes.empty()) {
                        // is_abort_index = true;
                        for (auto &index: indexes) {
                            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                            char *key = new char[index.col_tot_len];
                            for (size_t i = 0; i < index.col_num; ++i) {
                                auto offset = tab_.get_col(index.cols[i].name)->offset;
                                memcpy(key + index.cols[i].offset, record.GetRecord().data + offset, index.cols[i].len);
                            }
                            try {
                                ih->insert_entry(key, record.GetRid(), txn);
                            }catch (IndexEntryExistsError error) {}
                            ih->index_aborted_ = true;
                        }
                        for (const auto& lock_data_id : *txn->get_lock_set()) {
                            lock_manager_->calculate_gaps_on_table(lock_data_id);
                        }
                    }
                    break;
                case WType::UPDATE_TUPLE:
                    // 操作索引
                    if(!indexes.empty()) {
                        // is_abort_index = true;
                        for(auto& index: indexes) {
                            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                            char* old_key = new char[index.col_tot_len];
                            char* new_key = new char[index.col_tot_len];
                            for(size_t i = 0; i < index.col_num; ++i) {
                                auto offset = tab_.get_col(index.cols[i].name)->offset;
                                memcpy(old_key + index.cols[i].offset, record.GetOldRecord().data + offset, index.cols[i].len);
                            }
                            for(size_t i = 0; i < index.col_num; ++i) {
                                auto offset = tab_.get_col(index.cols[i].name)->offset;
                                memcpy(new_key + index.cols[i].offset, record.GetRecord().data + offset, index.cols[i].len);
                            }
                            ih->delete_entry(new_key, txn);
                            ih->insert_entry(old_key, record.GetRid(), txn);
                            ih->index_aborted_ = true;
                        }
                        for (const auto& lock_data_id : *txn->get_lock_set()) {
                            lock_manager_->calculate_gaps_on_table(lock_data_id);
                        }
                    }
                    // 回滚更新元组
                    if(record.GetOldRecord().allocated_) {
                        file_handle->update_record(record.GetRid(), record.GetOldRecord().data, nullptr);
                    }
                    break;
            }
            // delete record;
        }

    }

    // 释放所有锁
    for (const auto& lock_data_id : *txn->get_lock_set()) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 清空事务相关资源
    txn->get_lock_set()->clear();

    if (log_manager != nullptr) {
        auto record = AbortLogRecord(txn->get_transaction_id());
        record.prev_lsn_ = txn->get_prev_lsn();
        auto lsn = log_manager->add_log_to_buffer(&record);
        txn->set_prev_lsn(lsn);
    }

    // 更新事务状态并释放资源
//    std::unique_lock<std::mutex> lock(latch_);
//    txn_map.erase(txn->get_transaction_id());
//    lock.unlock();
    txn->set_state(TransactionState::ABORTED);
}