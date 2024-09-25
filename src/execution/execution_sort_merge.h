//
// Created by Pepephant on 2024/6/26.
//

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortMergeExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    /*My fields*/
    std::unique_ptr<RmRecord> record_;
    std::vector<ColMeta> left_cols_;
    std::vector<ColMeta> right_cols_;
    std::vector<ColMeta> all_cols_;
public:
    SortMergeExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds) {
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
        isend = false;
        fed_conds_ = std::move(conds);

        left_cols_ = left_->cols();
        right_cols_ = right_->cols();
        all_cols_.insert(all_cols_.end(), left_cols_.begin(), left_cols_.end());
        all_cols_.insert(all_cols_.end(), right_cols_.begin(), right_cols_.end());

        for (auto& cond: fed_conds_) {
            try {
                get_col(left_cols_, cond.lhs_col);
            } catch (ColumnNotFoundError& e) {
                // TODO: flip operands
            }
        }
    }

    void beginTuple() override {
        output_sorted(left_.get());
        output_sorted(right_.get());

        left_->beginTuple();
        right_->beginTuple();

        while (!left_->is_end() && !right_->is_end()) {
            auto left_tuple = left_->Next();
            auto right_tuple = right_->Next();

            for (auto& cond: fed_conds_) {
                auto left_val = getValue(cond.lhs_col, left_tuple.get());
                auto right_val = getValue(cond.rhs_col, right_tuple.get());
                if (Value::ValueComp(left_val,right_val, OP_LT)) {
                    left_->nextTuple();
                } else if (Value::ValueComp(left_val, right_val, OP_GT)) {
                    right_->nextTuple();
                } else {
                    auto tuple = make_record(left_tuple.get(), right_tuple.get());
                    record_ = std::move(tuple);
                    right_->nextTuple();
                    return ;
                }
            }
        }
        isend = true;
    }

    void nextTuple() override {
        while (!left_->is_end() && !right_->is_end()) {
            auto left_tuple = left_->Next();
            auto right_tuple = right_->Next();

            for (auto& cond: fed_conds_) {
                auto left_val = getValue(cond.lhs_col, left_tuple.get());
                auto right_val = getValue(cond.lhs_col, right_tuple.get());
                if (Value::ValueComp(left_val,right_val, OP_LT)) {
                    left_->nextTuple();
                } else if (Value::ValueComp(left_val, right_val, OP_GT)) {
                    right_->nextTuple();
                } else {
                    auto tuple = make_record(left_tuple.get(), right_tuple.get());
                    record_ = std::move(tuple);
                    right_->nextTuple();
                    return ;
                }
            }
        }
        isend = true;
    }

    bool is_end() const override {
        return isend;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(record_);
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    size_t tupleLen() const override {
        return len_;
    };

private:
    std::unique_ptr<RmRecord> make_record(RmRecord* tuple1, RmRecord* tuple2) {
        int new_size = tuple1->size + tuple2->size;
        RmRecord tuple(new_size);
        char* new_data = tuple.data;

        memcpy(new_data, tuple1->data, tuple1->size);
        memcpy(new_data + tuple1->size, tuple2->data, tuple2->size);

        return std::make_unique<RmRecord>(tuple);
    }

    Value getValue(TabCol col, RmRecord* tuple) {
        auto col_iter = get_col(all_cols_, col);
        auto col_meta = all_cols_[col_iter - all_cols_.begin()];
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

    void output_sorted(AbstractExecutor* executorTreeRoot) {
        std::vector<std::string> captions;
        captions.reserve(executorTreeRoot->cols().size());
        for (auto &sel_col : executorTreeRoot->cols()) {
            captions.push_back(sel_col.name);
        }

        std::fstream outfile;
        outfile.open("sorted_results.txt", std::ios::out | std::ios::app);

        outfile << "|";
        for(int i = 0; i < captions.size(); ++i) {
            outfile << " " << captions[i] << " |";
        }
        outfile << "\n";

        // 执行query_plan
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            auto Tuple = executorTreeRoot->Next();
            std::vector<std::string> columns;
            for (auto &col : executorTreeRoot->cols()) {
                std::string col_str;
                char *rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT) {
                    col_str = std::to_string(*(int *)rec_buf);
                } else if (col.type == TYPE_FLOAT) {
                    col_str = std::to_string(*(float *)rec_buf);
                } else if (col.type == TYPE_STRING) {
                    col_str = std::string((char *)rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                }
                columns.push_back(col_str);
            }
            // print record into file
            outfile << "|";
            for(int i = 0; i < columns.size(); ++i) {
                outfile << " " << columns[i] << " |";
            }
            outfile << "\n";
        }
        outfile.close();
    }
};
