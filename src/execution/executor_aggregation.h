//
// Created by Pepephant on 2024/5/31.
//
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include <cassert>

class AggregateExecutor : public AbstractExecutor {
private:
    struct AggrKey {
        std::vector<Value> values_;

        friend auto operator == (const AggrKey& x, const AggrKey& y) -> bool
        {
            assert(x.values_.size() == y.values_.size());

            for (int i = 0; i < x.values_.size(); i++) {
                if (Value::ValueComp(x.values_[i], y.values_[i], OP_NE)) {
                    return false;
                }
            }
            return true;
        }
    };

    struct AggrKeyHash {
        std::size_t operator()(const AggrKey& k) const
        {
            std::string hash_str;
            for (int i = 0; i < k.values_.size(); i++) {
                hash_str += k.values_[i].raw->data;
            }
            return std::hash<std::string>()(hash_str);
        }
    };

    struct AggrKeyEqual {
        auto operator()(const AggrKey& x, const AggrKey& y) const -> bool
        {
            return x == y;
        }
    };

    struct AggrValue {
        std::vector<Value> values_;
        std::vector<Value> havings_;
    };

private:
    std::unique_ptr<AbstractExecutor> prev_;    // 需要聚集节点的儿子节点
    std::vector<ColMeta> aggregates_;            // 表的数据文件句柄
    std::vector<AggrType> types_;
    std::vector<ColMeta> group_bys_;

    /* My fields */
    int len_;
    std::vector<ColMeta> cols_;
    std::vector<ColMeta> have_cols_;
    std::vector<HavingCond> havings_;
    std::unordered_multimap<AggrKey, AggrValue, AggrKeyHash, AggrKeyEqual> hash_table_;
    std::unordered_multimap<AggrKey, AggrValue, AggrKeyHash, AggrKeyEqual>::const_iterator hash_it_;

public:
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<AggrCol>& aggregates,
                      const std::vector<TabCol>& group_bys, const std::vector<HavingCond>& havings,
                      Context *context) {
        prev_ = std::move(prev);
        len_ = 0;

        for (auto& aggr: aggregates) {
            if (aggr.type_ == COUNT_STAR) {
                ColMeta agg_meta;
                agg_meta.type = TYPE_INT;
                agg_meta.len = sizeof(int);
                agg_meta.offset = len_;
                agg_meta.tab_name = "";
                agg_meta.name = aggr.alias_;
                len_ += agg_meta.len;
                types_.push_back(aggr.type_);
                aggregates_.emplace_back();
                cols_.push_back(agg_meta);
                continue;
            }

            auto it = prev_->get_col(prev_->cols(), aggr.tab_col_);

            types_.push_back(aggr.type_);
            aggregates_.push_back(*it);

            auto agg_meta = *it;
            agg_meta.tab_name = "";
            agg_meta.name = aggr.alias_;
            agg_meta.offset = len_;
            if (aggr.type_ == COUNT) {
                agg_meta.type = TYPE_INT;
                agg_meta.len = sizeof(int);
            } else if (agg_meta.type == TYPE_STRING) {
                if (aggr.type_ == SUM) {
                    throw InternalError("Can't aggregate SUM on a string column");
                }
            }
            len_ += agg_meta.len;
            cols_.push_back(agg_meta);
        }

        for (auto& group_by: group_bys) {
            auto it = prev_->get_col(prev_->cols(), group_by);
            group_bys_.push_back(*it);

            auto group_by_meta = *it;
            group_by_meta.offset = len_;
            len_ += group_by_meta.len;
            cols_.push_back(group_by_meta);
        }

        havings_ = havings;
        for (auto& having: havings) {
            auto aggr = having.lhs_col;
            if (aggr.type_ == COUNT_STAR) {
                have_cols_.emplace_back();
                continue;
            }

            auto it = prev_->get_col(prev_->cols(), aggr.tab_col_);
            have_cols_.push_back(*it);
        }

        context_ = context;
    }

    void beginTuple() override {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            AggrKey aggr_key;
            generate_aggr_key(record.get(), &aggr_key);
            if (hash_table_.count(aggr_key) == 0) {
                AggrValue value;
                init_aggr_value(record.get(), &value);
                hash_table_.insert(std::make_pair(aggr_key, value));
            }
            else if (hash_table_.count(aggr_key) == 1) {
                auto it = hash_table_.find(aggr_key);
                if (it->first == aggr_key) {
                    update_aggr_value(record.get(), &it->second);
                } else {
                    AggrValue value;
                    init_aggr_value(record.get(), &value);
                    hash_table_.insert(std::make_pair(aggr_key, value));
                    std::cout << "Hash conflict\n";
                }
            }
            else {
                auto range = hash_table_.equal_range(aggr_key);
                std::cout << "Hash conflict\n";
                for (auto it = range.first; it != range.second; it++) {
                    if (it->first == aggr_key) {
                        update_aggr_value(record.get(), &it->second);
                    }
                }
            }
        }

        std::vector<AggrKey> delete_keys;
        for (auto it = hash_table_.begin(); it != hash_table_.end(); it++) {
            if (!judge_having(it->second.havings_)) {
                delete_keys.push_back(it->first);
            }
        }

        for (auto& key: delete_keys) {
            hash_table_.erase(key);
        }

        if (hash_table_.empty() && group_bys_.empty() && only_count()) {
            RmRecord record;
            AggrKey aggr_key;
            AggrValue aggr_value;
            generate_aggr_key(&record, &aggr_key);
            init_aggr_value(nullptr, &aggr_value);
            hash_table_.insert(std::make_pair(aggr_key, aggr_value));
        }

        hash_it_ = hash_table_.begin();
    }

    void nextTuple() override {
        hash_it_++;
    }

    bool is_end() const override {
        return hash_it_ == hash_table_.end();
    }

    const std::vector<ColMeta> & cols() const override {
        return cols_;
    }

    std::unique_ptr<RmRecord> Next() override {
        auto agg_key = hash_it_->first;
        auto agg_val = hash_it_->second;
        RmRecord record(len_);
        size_t num_agg_col = aggregates_.size();

        for (int i = 0; i < num_agg_col; i++) {
            auto rec_size = cols_[i].len;
            auto rec_offset = cols_[i].offset;
            agg_val.values_[i].init_raw(rec_size);
            auto new_data = agg_val.values_[i].raw->data;
            memcpy(record.data + rec_offset, new_data, rec_size);
        }

        for (size_t i = num_agg_col, j = 0; i < cols_.size(); i++, j++) {
            auto rec_size = cols_[i].len;
            auto rec_offset = cols_[i].offset;
            auto new_data = agg_key.values_[j].raw->data;
            memcpy(record.data + rec_offset, new_data, rec_size);
        }

        return std::make_unique<RmRecord>(record);
    }

    size_t tupleLen() const override {
        return len_;
    }

    Rid &rid() override { return _abstract_rid; }

private:
    bool only_count() {
        for (auto& type: types_) {
            if (type != COUNT_STAR && type != COUNT) {
                return false;
            }
        }
        return !types_.empty();
    }

    void generate_aggr_key(RmRecord* record, AggrKey* key) {
        for (auto& group_by: group_bys_) {
            Value new_val;

            new_val.type = group_by.type;
            auto buf_size = group_by.len;
            auto buf_offset = group_by.offset;
            char* buf = new char[buf_size];
            new_val.init_raw(buf_size);
            memcpy(buf, record->data + buf_offset, buf_size);
            memcpy(new_val.raw->data, buf, buf_size);

            key->values_.push_back(new_val);
        }
    }

    void init_aggr_value(RmRecord* record, AggrValue* value){
        if (record == nullptr) {
            for (int i = 0; i < aggregates_.size(); i++) {
                Value new_val;
                new_val.type = TYPE_INT;
                new_val.int_val = 0;
                value->values_.push_back(new_val);
            }
            return;
        }

        for (int i = 0; i < aggregates_.size(); i++) {
            Value new_val;
            if (types_[i] == COUNT_STAR) {
                new_val.type = TYPE_INT;
                new_val.int_val = 1;
                value->values_.push_back(new_val);
                continue;
            }

            new_val.type = aggregates_[i].type;
            auto buf_size = aggregates_[i].len;
            auto buf_offset = aggregates_[i].offset;
            char* buf = new char[buf_size];
            memcpy(buf, record->data + buf_offset, buf_size);
            switch (types_[i]) {
                case COUNT:
                {
                    new_val.type = TYPE_INT;
                    new_val.int_val = 1;
                    break;
                }
                case MIN: case MAX: case SUM:
                {
                    if (aggregates_[i].type == TYPE_INT) {
                        new_val.type = TYPE_INT;
                        new_val.int_val = *(int*)(buf);
                    } else if (aggregates_[i].type == TYPE_FLOAT) {
                        new_val.type = TYPE_FLOAT;
                        new_val.float_val = *(float*)(buf);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                default: break;
            }
            value->values_.push_back(new_val);
        }

        for (int i = 0; i < have_cols_.size(); i++) {
            Value new_val;
            auto have_agg_col = havings_[i].lhs_col;
            if (have_agg_col.type_ == COUNT_STAR) {
                new_val.type = TYPE_INT;
                new_val.int_val = 1;
                value->havings_.push_back(new_val);
                continue;
            }

            new_val.type = have_cols_[i].type;
            auto buf_size = have_cols_[i].len;
            auto buf_offset = have_cols_[i].offset;
            char* buf = new char[buf_size];
            memcpy(buf, record->data + buf_offset, buf_size);
            switch (have_agg_col.type_) {
                case COUNT:
                {
                    new_val.type = TYPE_INT;
                    new_val.int_val = 1;
                    break;
                }
                case MIN: case MAX: case SUM:
                {
                    if (have_cols_[i].type == TYPE_INT) {
                        new_val.type = TYPE_INT;
                        new_val.int_val = *(int*)(buf);
                    } else if (have_cols_[i].type == TYPE_FLOAT) {
                        new_val.type = TYPE_FLOAT;
                        new_val.float_val = *(float*)(buf);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                default: break;
            }
            value->havings_.push_back(new_val);
        }
    }

    void update_aggr_value(RmRecord* record, AggrValue* value){
        for (int i = 0; i < aggregates_.size(); i++) {
            Value new_val = value->values_[i];
            auto buf_size = aggregates_[i].len;
            auto buf_offset = aggregates_[i].offset;
            char* buf = new char[buf_size];
            memcpy(buf, record->data + buf_offset, buf_size);
            switch (types_[i]) {
                case COUNT: case COUNT_STAR:
                {
                    new_val.int_val++;
                    break;
                }
                case SUM:
                {
                    if (aggregates_[i].type == TYPE_INT) {
                        new_val.int_val += *(int*)(buf);
                    } else if (aggregates_[i].type == TYPE_FLOAT) {
                        new_val.float_val += *(float*)(buf);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                case MIN:
                {
                    if (aggregates_[i].type == TYPE_INT) {
                        new_val.int_val = std::min(*(int*)(buf), new_val.int_val);
                    } else if (aggregates_[i].type == TYPE_FLOAT) {
                        new_val.float_val = std::min(*(float*)(buf), new_val.float_val);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                case MAX:
                {
                    if (aggregates_[i].type == TYPE_INT) {
                        new_val.int_val = std::max(*(int*)(buf), new_val.int_val);
                    } else if (aggregates_[i].type == TYPE_FLOAT) {
                        new_val.float_val = std::max(*(float*)(buf), new_val.float_val);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                default: break;
            }
            value->values_[i] = new_val;
        }

        for (int i = 0; i < have_cols_.size(); i++) {
            Value new_val = value->havings_[i];
            auto buf_size = have_cols_[i].len;
            auto buf_offset = have_cols_[i].offset;
            char* buf = new char[buf_size];
            memcpy(buf, record->data + buf_offset, buf_size);
            switch (havings_[i].lhs_col.type_) {
                case COUNT: case COUNT_STAR:
                {
                    new_val.int_val++;
                    break;
                }
                case SUM:
                {
                    if (have_cols_[i].type == TYPE_INT) {
                        new_val.int_val += *(int*)(buf);
                    } else if (have_cols_[i].type == TYPE_FLOAT) {
                        new_val.float_val += *(float*)(buf);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                case MIN:
                {
                    if (have_cols_[i].type == TYPE_INT) {
                        new_val.int_val = std::min(*(int*)(buf), new_val.int_val);
                    } else if (have_cols_[i].type == TYPE_FLOAT) {
                        new_val.float_val = std::min(*(float*)(buf), new_val.float_val);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                case MAX:
                {
                    if (have_cols_[i].type == TYPE_INT) {
                        new_val.int_val = std::max(*(int*)(buf), new_val.int_val);
                    } else if (have_cols_[i].type == TYPE_FLOAT) {
                        new_val.float_val = std::max(*(float*)(buf), new_val.float_val);
                    } else {
                        throw InternalError("Aggregation on string Not supported!");
                    }
                    break;
                }
                default: break;
            }
            value->havings_[i] = new_val;
        }
    }

    bool judge_having(std::vector<Value>& havings) {
        for (int i = 0; i < havings.size(); i++) {
            if (!Value::ValueComp(havings[i], havings_[i].rhs_val, havings_[i].op)) {
                return false;
            }
        }
        return true;
    }
};
