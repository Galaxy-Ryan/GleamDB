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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;
    /*My parameter*/
    bool is_end_;
    Rid last_rid_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        std::cout << "Try initializing SeqScanExecutor" << '\n';
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;

        for (auto& cond: conds_) {
            ColType lhs = tab.get_col(cond.lhs_col.col_name)->type;
            ColType rhs = cond.rhs_val.type;
            if (!Value::TypeCompatible(lhs, rhs)) {
                throw IncompatibleTypeError(coltype2str(lhs), coltype2str(rhs));
            }
        }

        context_->lock_mgr_->lock_IS_on_table(context_->txn_,fh_->GetFd());
    }

    void beginTuple() override {

        scan_ = std::make_unique<RmScan>(fh_);
        rid_ = scan_->rid();
        while (!scan_->is_end()) {
            auto tuple = fh_->get_record(rid_, context_);
            if (eval_condition(conds_, tuple.get())) {
                last_rid_ = scan_->rid();
            }
            scan_->next();
            rid_ = scan_->rid();
        }

        scan_ = std::make_unique<RmScan>(fh_);
        is_end_ = false;
        rid_ = scan_->rid();

        while (!scan_->is_end()) {
            auto tuple = fh_->get_record(rid_, context_);
            scan_->next();
            if (eval_condition(conds_, tuple.get())) {
                return ;
            }
            rid_ = scan_->rid();
        }

        is_end_ = true;
    }

    void nextTuple() override {

        if (last_rid_ == rid_) {
            is_end_ = true;
            return ;
        }

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto tuple = fh_->get_record(rid_, context_);
            scan_->next();
            if (eval_condition(conds_, tuple.get())) {
                return ;
            }
        }
    }

    bool is_end() const override {
        return is_end_;
    }

    const std::vector<ColMeta> & cols() const override {
        return cols_;
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    size_t tupleLen() const override {
        return len_;
    }

    Rid &rid() override { return rid_; }

private:
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

    Value getValue(TabCol col, RmRecord* tuple) {
        auto col_iter = get_col(cols_, col);
        auto col_meta = cols_[col_iter - cols_.begin()];
        char* col_raw = new char[col_meta.len];
        memset(col_raw, 0, col_meta.len);
        memcpy(col_raw, tuple->data + col_meta.offset, col_meta.len);

        Value val{};
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