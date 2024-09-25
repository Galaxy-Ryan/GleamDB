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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    bool left_end_;
    bool right_end_;
    bool is_last_tuple_;

    /*My fields*/
    std::unique_ptr<RmRecord> record_;
    std::vector<ColMeta> left_cols_;
    std::vector<ColMeta> right_cols_;
    std::vector<ColMeta> all_cols_;

    std::unique_ptr<RmRecord> left_tuple_;
    std::unique_ptr<RmRecord> right_tuple_;
   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        std::swap(left_, right_);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        is_last_tuple_ = false;
        left_end_ = false;
        right_end_ = false;
        isend = false;
        fed_conds_ = std::move(conds);

        left_cols_ = left_->cols();
        right_cols_ = right_->cols();
        all_cols_.insert(all_cols_.end(), left_cols_.begin(), left_cols_.end());
        all_cols_.insert(all_cols_.end(), right_cols_.begin(), right_cols_.end());
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();

        left_end_ = left_->is_end();
        right_end_ = right_->is_end();
        if (left_end_ || right_end_) {
            is_last_tuple_ = true;
            isend = true;
            return;
        }

        while (!is_last_tuple_) {
            left_tuple_ = left_->Next();
            right_tuple_ = right_->Next();
            auto tuple = make_record(left_tuple_.get(), right_tuple_.get());

            // std::cout << "Now at: " << left_->rid().toString() << ", " << right_->rid().toString() << "\n";

            left_end_ = left_->is_end();
            right_end_ = right_->is_end();

            right_->nextTuple();
            right_end_ = right_->is_end();
            if (right_end_) {
                left_->nextTuple();
                right_->beginTuple();
                left_end_ = left_->is_end();
                if (left_end_) {
                    isend = true;
                    is_last_tuple_ = true;
                }
            }

            if (evaluate_join(tuple.get())) {
                record_ = std::move(tuple);
                if (is_last_tuple_) {
                    isend = false;
                }
                break;
            }
        }
    }

    void nextTuple() override {
        if (is_last_tuple_ && !isend) {
            isend = true;
        }

        while (!is_last_tuple_) {
            left_tuple_ = left_->Next();
            right_tuple_ = right_->Next();
            auto tuple = make_record(left_tuple_.get(), right_tuple_.get());

            // std::cout << "Now at: " << left_->rid().toString() << ", " << right_->rid().toString() << "\n";

            left_end_ = left_->is_end();
            right_end_ = right_->is_end();

            right_->nextTuple();
            right_end_ = right_->is_end();
            if (right_end_) {
                left_->nextTuple();
                right_->beginTuple();
                left_end_ = left_->is_end();
                if (left_end_) {
                    isend = true;
                    is_last_tuple_ = true;
                }
            }

            if (evaluate_join(tuple.get())) {
                record_ = std::move(tuple);
                if (is_last_tuple_) {
                    isend = false;
                }
                break;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(record_);
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override {
        return isend;
    }

    const std::vector<ColMeta> & cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

private:
    bool evaluate_join(RmRecord* tuple) {

        for (auto& cond: fed_conds_) {
            auto left = getValue(cond.lhs_col, tuple);
            Value right{};
            if (cond.is_rhs_val) {
                right = cond.rhs_val;
            } else {
                right = getValue(cond.rhs_col, tuple);
            }

            bool pred_cond = Value::ValueComp(left, right, cond.op);

            if (!pred_cond) { return false; }
        }
        return true;
    }

    std::unique_ptr<RmRecord> make_record(RmRecord* tuple1, RmRecord* tuple2) {
        int new_size = tuple1->size + tuple2->size;
        RmRecord tuple(new_size);
        char* new_data = tuple.data;

        memcpy(new_data, tuple1->data, tuple1->size);
        memcpy(new_data + tuple1->size, tuple2->data, tuple2->size);

        return std::make_unique<RmRecord>(tuple);
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
};