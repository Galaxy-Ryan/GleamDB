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

#include <cmath>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    std::unique_ptr<IxScan> ix_scan_;
    IxIndexHandle* ih_;

    SmManager *sm_manager_;
    bool is_end_;


public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                      Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }

        for (auto& cond : conds_) {
            ColMeta col_meta = *tab_.get_col(cond.lhs_col.col_name);
            ColType lhs = col_meta.type;
            ColType rhs = cond.rhs_val.type;
            if (!Value::TypeCompatible(col_meta.type, cond.rhs_val.type)) {
                throw IncompatibleTypeError(coltype2str(lhs), coltype2str(rhs));
            }
            if (cond.rhs_val.type == TYPE_INT && col_meta.type == TYPE_FLOAT) {
                cond.rhs_val.type = TYPE_FLOAT;
                cond.rhs_val.float_val = static_cast<float>(cond.rhs_val.int_val);
                cond.rhs_val.raw = nullptr;
                cond.rhs_val.init_raw(col_meta.len);
            }
        }

        context_->lock_mgr_->lock_IS_on_table(context_->txn_,fh_->GetFd());

        fed_conds_ = conds_;
        is_end_ = false;
    }

    void beginTuple() override {
        auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        ih_ = sm_manager_->ihs_.at(ix_name).get();
        auto bpm = sm_manager_->get_bpm();
        Iid lower;
        Iid upper;

        do {
            ih_->index_aborted_ = false;

            auto lower_key = new char[index_meta_.col_tot_len];
            auto upper_key = new char[index_meta_.col_tot_len];
            auto bound = get_bound(lower_key, upper_key);
            lower = bound.first;
            upper = bound.second;
            get_gap_lock_key(lower_key, upper_key, bound);

            is_end_ = ix_compare(lower_key, upper_key, index_meta_.cols) > 0;
            if (is_end_) { break; }

            context_->lock_mgr_->lock_GAP_on_table(context_->txn_,fh_->GetFd(),lower_key,upper_key,index_meta_,conds_,ih_);
        } while (ih_->index_aborted_);

        ix_scan_ = std::make_unique<IxScan>(ih_, lower, upper, bpm);
        if (!ix_scan_->is_end() && !is_end_) {
            rid_ = ix_scan_->rid();
        }
    }

    void nextTuple() override {
        ix_scan_->next();
        if (!ix_scan_->is_end()) {
            rid_ = ix_scan_->rid();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, nullptr);
    }

    bool is_end() const override { return ix_scan_->is_end() || is_end_; };

    size_t tupleLen() const override { return len_; };

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    Rid &rid() override { return rid_; }

private:

    inline bool check_valid(const char* lower, const char* upper) {
        return ix_compare(lower, upper, index_meta_.cols) > 0;
    }

    inline std::pair<Iid, Iid> get_bound(char* lower_key, char* upper_key) {
        if (conds_.empty()) {
            int offset = 0;
            for (auto& col: index_meta_.cols) {
                Value value;
                value.generate_max(col.type, col.len);
                memcpy(upper_key + offset, value.raw->data, col.len);
                offset += col.len;
            }

            offset = 0;
            for (auto& col: index_meta_.cols) {
                Value value;
                value.generate_min(col.type, col.len);
                memcpy(lower_key + offset, value.raw->data, col.len);
                offset += col.len;
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
            upper_bound = ih_->lower_bound(upper_values);
        } else {
            upper_bound = ih_->upper_bound(upper_values);
        }

        if (lower_comp == OP_GT) {
            lower_bound = ih_->upper_bound(lower_values);
        } else {
            lower_bound = ih_->lower_bound(lower_values);
        }

        return std::make_pair(lower_bound, upper_bound);
    }

    void inline get_gap_lock_key(char* lower_key, char* upper_key, std::pair<Iid, Iid> bound) {
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
            print_key(lower_key, "lower_key");
            print_key(lower_bound, "lower_bound");
            assert(lower_comp == 0);
            memcpy(lower_key, lower_bound, index_meta_.col_tot_len);
        }

        memcpy(upper_key, upper_bound, index_meta_.col_tot_len);
    }

    void print_key(const char* key, std::string description) {
        std::cout << description << ": ";
        int offset = 0;
        for (auto& col: index_meta_.cols) {
            switch (col.type) {
                case TYPE_INT:
                {
                    int value = *(int*)(key + offset);
                    std::cout << value << " ";
                    break;
                }
                case TYPE_FLOAT:
                {
                    float value = *(float*)(key + offset);
                    std::cout << value << " ";
                    break;
                }
                case TYPE_STRING:
                {
                    std::string value = std::string(key + offset, col.len);
                    value.resize(strlen(key + offset));
                    std::cout << value << " ";
                    break;
                }
            }
            offset += col.len;
        }
        std::cout << '\n';
    }
};