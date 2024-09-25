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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols, const std::vector<TabCol> &cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        for (int i = 0; i < cols.size(); i++) {
            cols_[i].tab_name = cols_[i].tab_name;
            cols_[i].name = cols[i].col_name;
        }
        len_ = curr_offset;
    }

    void beginTuple() override {
        prev_->beginTuple();
        _abstract_rid = prev_->rid();
    }

    void nextTuple() override {
        prev_->nextTuple();
        _abstract_rid = prev_->rid();
    }

    bool is_end() const override {
        return prev_->is_end();
    }

    const std::vector<ColMeta> & cols() const override {
        return cols_;
    }

    std::unique_ptr<RmRecord> Next() override {
        char* prev_data = new char[prev_->tupleLen()];
        memcpy(prev_data, prev_->Next()->data, prev_->tupleLen());
        RmRecord rec(static_cast<int>(len_));

        auto &prev_cols = prev_->cols();
        for (int i = 0; i < cols_.size(); i++) {
            memset(rec.data + cols_[i].offset, 0, cols_[i].len);
            memcpy(rec.data + cols_[i].offset, prev_data + prev_cols[sel_idxs_[i]].offset, cols_[i].len);
        }

        return std::make_unique<RmRecord>(rec);
    }

    Rid &rid() override { return _abstract_rid; }
};