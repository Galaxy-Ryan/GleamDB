//
// Created by Pepephant on 2024/6/24.
//

#include "executor_abstract.h"
#include "record/rm_scan.h"

class SubqueryExecutor : public AbstractExecutor{
public:
    SubqueryExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, SubQueryClause sub_clause,
                     std::unique_ptr<AbstractExecutor> subquery) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        subquery_ = std::move(subquery);
        sub_clause_ = std::move(sub_clause);

        auto sub_meta = subquery_->cols();
        if (sub_meta.size() != 1) {
            throw InternalError("Subquery return data longer than one");
        }

        auto left_type = tab.get_col(sub_clause_.lhs.col_name)->type;
        if (!Value::TypeCompatible(left_type, sub_meta[0].type)) {
            throw IncompatibleTypeError(coltype2str(left_type), coltype2str(sub_meta[0].type));
        }
    }

    void beginTuple() override {
        auto sub_meta = subquery_->cols();

        for (subquery_->beginTuple(); !subquery_->is_end(); subquery_->nextTuple()) {
            auto record = subquery_->Next();
            Value value;
            value.type = sub_meta[0].type;
            switch (value.type) {
                case TYPE_INT:
                    value.int_val = *(int*)(record->data);
                case TYPE_FLOAT:
                    value.float_val = *(float*)(record->data);
                case TYPE_STRING:
                    value.str_val = std::string(record->data);
            }
            value.init_raw(sub_meta[0].len);
            sub_results_.push_back(value);
        }

        if (!sub_clause_.in_clause && sub_results_.size() != 1) {
            throw InternalError("Can't compare results from subquery longer than one");
        }

        scan_ = std::make_unique<RmScan>(fh_);
        rid_ = scan_->rid();

        while (!scan_->is_end()) {
            auto tuple = fh_->get_record(rid_, context_);
            if (eval_condition(tuple.get())) {
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
            if (eval_condition(tuple.get())) {
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
            if (eval_condition(tuple.get())) {
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
    SmManager *sm_manager_;
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator
    /*My parameter*/
    bool is_end_;
    Rid last_rid_;

    SubQueryClause sub_clause_;
    std::vector<Value> sub_results_;
    std::unique_ptr<AbstractExecutor> subquery_;

private:
    bool eval_condition(RmRecord* tuple) {
        if (!eval_subquery(tuple)) {
            return false;
        }
        for (auto& cond: conds_) {
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

    bool eval_subquery(RmRecord* tuple) {
        auto left_val = getValue(sub_clause_.lhs, tuple);
        if (sub_clause_.in_clause) {
            auto it = std::find_if(sub_results_.begin(), sub_results_.end(), [&left_val](const Value& sub_res) {
                return Value::ValueComp(left_val, sub_res, OP_EQ);
            });
            return it != sub_results_.end();
        }
        return Value::ValueComp(left_val, sub_results_[0], sub_clause_.op);
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
