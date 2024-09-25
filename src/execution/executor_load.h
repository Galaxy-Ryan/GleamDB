//
// Created by Pepephant on 2024/7/29.
//

#pragma once

#include <sstream>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class LoadExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                   // 表的元数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    std::string path_;              // 源文件
    SmManager *sm_manager_;
    Context* context_;

public:
    LoadExecutor(SmManager *sm_manager, const std::string &path, const std::string &tab_name, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        context_ = context;
        path_ = path;

        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        tab_ = sm_manager_->db_.get_table(tab_name_);

        // if(context_->txn_->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
        //     auto fd = sm_manager_->fhs_.at(tab_name_).get()->GetFd();
        //     context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,fd);
        // }
    };

    std::unique_ptr<RmRecord> Next() override {
        std::ifstream ifs;
        ifs.open(path_);
        if(!ifs.is_open()) {
            throw UnixError();
        }

        std::string line;
        std::getline(ifs,line);

        while(std::getline(ifs,line)) {
            std::vector<std::string> row;
            std::stringstream ss(line);
            std::string col;
            while (std::getline(ss, col, ',')) {
                row.push_back(col);
            }

            std::vector<Value> values;
            for (size_t i = 0; i < row.size(); i++) {//遍历一行的数据
                auto &col_meta = tab_.cols[i];
                auto &val = row[i];
                Value value;
                if(col_meta.type==ColType::TYPE_INT) {
                    value.set_int(std::stoi(val));
                }else if(col_meta.type==ColType::TYPE_FLOAT) {
                    value.set_float(std::stof(val));
                }else if(col_meta.type==ColType::TYPE_STRING) {
                    value.set_str(val);
                }else if(col_meta.type==ColType::TYPE_DATETIME) {
                    value.set_datetime(val);
                }
                value.init_raw(col_meta.len);
                values.push_back(value);
            }

            RmRecord rec(fh_->get_file_hdr().record_size);

            for (size_t i = 0; i < values.size(); i++) {
                auto &col_meta = tab_.cols[i];
                memcpy(rec.data + col_meta.offset, values[i].raw->data, col_meta.len);
            }

            auto rid = fh_->insert_record(rec.data, context_);

            auto writeRecord = WriteRecord(WType::INSERT_TUPLE, tab_.name, rid, rec);
            context_->txn_->append_write_record(writeRecord);

            for (auto &index: tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                for (size_t i = 0; i < index.col_num; ++i) {
                    auto offset = tab_.get_col(index.cols[i].name)->offset;
                    memcpy(key + index.cols[i].offset, rec.data + offset, index.cols[i].len);
                }
                try {
                    ih->insert_entry(key, rid, context_->txn_);
                } catch (IndexEntryExistsError& error) {
                    fh_->delete_record(rid, context_);
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
            InsertLogRecord log(txn_id, prev_lsn, rec, rid, tab_name_, first_page, num_pages);
            context_->log_mgr_->add_log_to_buffer(&log);
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};