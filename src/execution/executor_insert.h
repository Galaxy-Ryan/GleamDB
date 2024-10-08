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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // 直接锁表，解决幻读
        if(tab_.indexes.empty()) {
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,fh_->GetFd());
        } else {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_,fh_->GetFd());
        }
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.type = TYPE_FLOAT;
                    val.float_val = val.int_val;
                } else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);

        auto writeRecord = WriteRecord(WType::INSERT_TUPLE, tab_.name, rid_, rec);
        context_->txn_->append_write_record(writeRecord);

        // Insert into index
        for (auto &index: tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            for (size_t i = 0; i < index.col_num; ++i) {
                auto offset = tab_.get_col(index.cols[i].name)->offset;
                memcpy(key + index.cols[i].offset, rec.data + offset, index.cols[i].len);
            }
            context_->lock_mgr_->lock_exclusive_on_record_gap(context_->txn_,rid_,fh_->GetFd(),key,index,ih);
            try {
                // TODO 处理并发上锁
                ih->insert_entry(key, rid_, context_->txn_);
            } catch (IndexEntryExistsError& error) {
                fh_->delete_record(rid_, context_);
                context_->txn_->get_write_set()->pop_back();
                context_->txn_->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                AbortReason::CONSTRAINT_VIOLATION,
                                                error._msg);
            }
        }

        txn_id_t txn_id = context_->txn_->get_transaction_id();
        lsn_t prev_lsn = context_->txn_->get_prev_lsn();
        context_->txn_->set_prev_lsn(context_->log_mgr_->get_lsn());

        int first_page = fh_->get_file_hdr().first_free_page_no;
        int num_pages = fh_->get_file_hdr().num_pages;
        InsertLogRecord log(txn_id, prev_lsn, rec, rid_, tab_name_, first_page, num_pages);
        context_->log_mgr_->add_log_to_buffer(&log);

        return nullptr;
    }

    Rid &rid() override { return rid_; }
};