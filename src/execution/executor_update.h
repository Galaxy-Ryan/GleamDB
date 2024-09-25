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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    /*My fields*/
    std::vector<ColMeta> cols_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        cols_ = tab_.cols;
        context_ = context;
        context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,fh_->GetFd());
    }

    std::unique_ptr<RmRecord> Next() override {
        for (auto& rid: rids_) {
            // if(context_->lock_mgr_->lock_exclusive_on_record(context_->txn_,rid,fh_->GetFd())){}
            auto tuple = fh_->get_record(rid, context_);
            RmRecord old_rec = *(tuple);
            RmRecord new_rec = *(tuple);

            if (!eval_condition(conds_, tuple.get())) {
                continue;
            }

            for (auto &clause: set_clauses_) {
                auto new_val = clause.rhs;
                auto col_meta = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &col) {
                    return col.name == clause.lhs.col_name;
                });
                auto offset = col_meta->offset;
                new_val.init_raw(col_meta->len);
                memcpy(new_rec.data + offset, new_val.raw->data, col_meta->len);
            }

            // update the index
            for(int ix_no = 0; ix_no < tab_.indexes.size(); ix_no++) {
                auto index = tab_.indexes[ix_no];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* old_key = new char[index.col_tot_len];
                char* new_key = new char[index.col_tot_len];
                for(size_t i = 0; i < index.col_num; ++i) {
                    auto offset = tab_.get_col(index.cols[i].name)->offset;
                    memcpy(old_key + index.cols[i].offset, old_rec.data + offset, index.cols[i].len);
                }
                for(size_t i = 0; i < index.col_num; ++i) {
                    auto offset = tab_.get_col(index.cols[i].name)->offset;
                    memcpy(new_key + index.cols[i].offset, new_rec.data + offset, index.cols[i].len);
                }
                try {
                    ih->delete_entry(old_key, context_->txn_);
                    ih->insert_entry(new_key, rid, context_->txn_);
                } catch (IndexEntryExistsError& error) {
                    for (int j = 0; j <= ix_no; j++) {
                        index = tab_.indexes[j];
                        ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                        char* key = new char[index.col_tot_len];
                        for (size_t i = 0; i < index.col_num; ++i) {
                            auto offset = tab_.get_col(index.cols[i].name)->offset;
                            memcpy(key + index.cols[i].offset, old_rec.data + offset, index.cols[i].len);
                        }
                        ih->insert_entry(key, rid, context_->txn_);
                    }
                    context_->txn_->set_state(TransactionState::ABORTED);
                    throw TransactionAbortException(context_->txn_->get_transaction_id(),
                                                    AbortReason::CONSTRAINT_VIOLATION,
                                                    error._msg);
                }
            }

            fh_->update_record(rid, new_rec.data, context_);
            WriteRecord writeRecord = WriteRecord(WType::UPDATE_TUPLE,tab_name_,rid,new_rec,old_rec);
            context_->txn_->append_write_record(writeRecord);

            txn_id_t txn_id = context_->txn_->get_transaction_id();
            lsn_t prev_lsn = context_->txn_->get_prev_lsn();
            context_->txn_->set_prev_lsn(context_->log_mgr_->get_lsn());

            int first_page = fh_->get_file_hdr().first_free_page_no;
            int num_pages = fh_->get_file_hdr().num_pages;
            UpdateLogRecord log(txn_id, prev_lsn, old_rec, new_rec, rid, tab_name_, first_page, num_pages);
            context_->log_mgr_->add_log_to_buffer(&log);
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

private:
    Value getValue(TabCol col, RmRecord* tuple) {
        auto col_iter = get_col(cols_, col);
        auto col_meta = cols_[col_iter - cols_.begin()];
        char* col_raw = new char[col_meta.len];
        memset(col_raw, 0, col_meta.len);
        memcpy(col_raw, tuple->data + col_meta.offset, col_meta.len);

        Value val;
        val.type = col_meta.type;

        if (col_meta.type == TYPE_INT) {
            auto int_val = *(int *)(col_raw);
            val.int_val = int_val;
        } else if (col_meta.type == TYPE_FLOAT) {
            auto float_val = *(float *)(col_raw);
            val.float_val = float_val;
        } else if (col_meta.type == TYPE_STRING) {
            auto str_val = std::string((char *)col_raw, col_meta.len);
            str_val.resize(strlen(str_val.c_str()));
            val.str_val = str_val;
        }

        return val;
    }

    bool eval_condition(std::vector<Condition> conds, RmRecord* tuple) {
        for (auto& cond: conds_) {
            auto left = getValue(cond.lhs_col, tuple);
            Value right{};
            if (cond.is_rhs_val) {
                right = cond.rhs_val;
            } else {
                right = getValue(cond.rhs_col, tuple);
            }

            bool pred_cond = false;
            if (left.type == TYPE_INT && right.type == TYPE_INT) {
                pred_cond = ValueComp(left.int_val, right.int_val, cond.op);
            } else if (left.type == TYPE_FLOAT && right.type == TYPE_INT) {
                pred_cond = ValueComp(left.float_val, static_cast<float>(right.int_val), cond.op);
            } else if (left.type == TYPE_INT && right.type == TYPE_FLOAT) {
                pred_cond = ValueComp(static_cast<float>(left.int_val), right.float_val, cond.op);
            } else if (left.type == TYPE_FLOAT && right.type == TYPE_FLOAT) {
                pred_cond = ValueComp(left.float_val, right.float_val, cond.op);
            } else if (left.type == TYPE_STRING && right.type == TYPE_STRING) {
                pred_cond = ValueComp(left.str_val, right.str_val, cond.op);
            } else {
                auto lhs = cond.lhs_col.tab_name + ":" + cond.lhs_col.col_name;
                auto rhs = cond.rhs_col.tab_name + ":" + cond.rhs_col.col_name;
                throw IncompatibleTypeError(lhs, rhs);
            }

            if (!pred_cond) { return false; }
        }
        return true;
    }

    inline bool ValueComp(int left, int right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }

    inline bool ValueComp(float left, float right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }

    inline bool ValueComp(std::string left, std::string right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }
};