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

#include <unistd.h>
#include <cmath>

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    std::vector<ColMeta> order_bys_;    // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    bool is_desc_;
//    std::vector<size_t> used_tuple;
//    std::unique_ptr<RmRecord> current_tuple;

    RmManager* rm_manager_;
    BufferPoolManager* bpm_;
    std::vector<page_id_t> page_ids_;

    int tmp_fd_;
    int cur_tup_;
    int num_pages_;
    int tuple_num_;
    int tuple_len_;
    int tup_per_page_;
    int block_size_min_;
    std::string tab_name_;

    int prev_district_;
    int curr_district_;

    struct RecordSort{
        std::vector<Value> orders_;
        RmRecord rec_;
        bool is_desc_;

        friend bool operator < (const RecordSort& r1, const RecordSort& r2) {
            assert(r1.orders_.size() == r2.orders_.size());
            for (size_t i = 0; i < r1.orders_.size(); i++) {
                if (Value::ValueComp(r1.orders_.at(i), r2.orders_.at(i), OP_EQ)) {
                    continue;
                }
                if (r1.is_desc_) {
                    return Value::ValueComp(r1.orders_.at(i), r2.orders_.at(i), OP_GT);
                }
                return Value::ValueComp(r1.orders_.at(i), r2.orders_.at(i), OP_LT);
            }
            return false;
        }
    };

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc, SmManager* sm_manager) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        order_bys_.push_back(*(prev_->get_col(cols_, sel_cols)));
        is_desc_ = is_desc;
        // used_tuple.clear();

        tuple_len_ = static_cast<int>(prev_->tupleLen());
        tab_name_ = sel_cols.tab_name;
        rm_manager_ = sm_manager->get_rm_manager();
        bpm_ = sm_manager->get_bpm();
        tup_per_page_ = PAGE_SIZE / tuple_len_;
        block_size_min_ = 100;
    }

    void beginTuple() override {
        std::vector<RecordSort> records;
        cur_tup_ = 0;
        tuple_num_ = 0;

        tab_name_ = get_tmp_name(order_bys_);
        tab_name_ = rm_manager_->create_tmp_file(tab_name_);
        tmp_fd_ = rm_manager_->open_tmp_file(tab_name_);

        tup_per_page_ = PAGE_SIZE / tuple_len_;
        int tup_per_block = block_size_min_ * tup_per_page_;

        for (prev_->beginTuple(); !prev_->is_end();) {
            records.clear();
            for (int i = 0; i < tup_per_block && !prev_->is_end(); i++) {
                auto tuple = prev_->Next();
                prev_->nextTuple();
                RecordSort rec_sort = {
                        .orders_ = getOrder(tuple->data),
                        .rec_ = *(tuple),
                        .is_desc_ = is_desc_
                };
                records.push_back(rec_sort);
            }
            std::sort(records.begin(), records.end());
            for (auto& rec: records) {
                insert_tuple(tuple_num_, rec.rec_.data, 0);
                tuple_num_++;
            }
        }

        num_pages_ = ceil_int(tuple_num_, tup_per_page_);

        if (num_pages_ == 1) {
            curr_district_ = 0;
            return ;
        }

        char* buf = new char[tuple_len_];
        memset(buf, 0, tuple_len_);
        for (int i = 0; i < tuple_num_; i++) {
            insert_tuple(i, buf, 1);
        }

        prev_district_ = 0;
        curr_district_ = 1;

        for (int block_size = block_size_min_; block_size < num_pages_; block_size *= 2) {
            tup_per_block = block_size * tup_per_page_;
            int offset = 0;
            int left_offset = 0;
            int right_offset = tup_per_block;
            int num_block = ceil_int(num_pages_, block_size);

            for (int bid = 0; bid < num_block; bid += 2) {
                int left_end = std::min(tuple_num_, (bid + 1) * tup_per_block);
                int right_end = std::min(tuple_num_, (bid + 2) * tup_per_block);
                if (left_end == right_end) {
                    while (offset < left_end) {
                        char* data = get_tuple(offset, prev_district_);
                        // outfile << "offset = " << offset << ", data = " << *(int*)data << '\n';
                        modify_tuple(offset++, data, curr_district_);
                    }
                    break;
                }
                while (left_offset < left_end || right_offset < right_end) {
                    char* left_data; char* right_data;
                    if (left_offset >= left_end) {
                        right_data = get_tuple(right_offset, prev_district_);
                        // outfile << "right_offset = " << right_offset << ", offset = " << offset << ", data = " << *(int*)left_data << '\n';
                        modify_tuple(offset++, right_data, curr_district_);
                        right_offset++; continue;
                    }
                    if (right_offset >= right_end) {
                        left_data = get_tuple(left_offset, prev_district_);
                        // outfile << "left_offset = " << left_offset << ", offset = " << offset << ", data = " << *(int*)right_data << '\n';
                        modify_tuple(offset++, left_data, curr_district_);
                        left_offset++; continue;
                    }
                    left_data = get_tuple(left_offset, prev_district_);
                    right_data = get_tuple(right_offset, prev_district_);
                    auto order = judge_order(left_data, right_data);
                    if (order == OP_LT) {
                        // outfile << "right_offset = " << left_offset << ", offset = " << offset << ", data = " << *(int*)left_data << '\n';
                        modify_tuple(offset++, left_data, curr_district_);
                        left_offset++;
                    } else if (order == OP_GT) {
                        // outfile << "right_offset = " << right_offset << ", offset = " << offset << ", data = " << *(int*)right_data << '\n';
                        modify_tuple(offset++, right_data, curr_district_);
                        right_offset++;
                    } else if (order == OP_EQ) {
                        modify_tuple(offset, left_data, curr_district_);
                        modify_tuple(offset, right_data, curr_district_);
                        left_offset++;  right_offset++; offset++;
                    }
                }

                left_offset += tup_per_block;
                right_offset += tup_per_block;
            }
            std::swap(prev_district_, curr_district_);

            // outfile << '\n';
        }
        std::swap(prev_district_, curr_district_);

        // outfile.close();
    }

    void nextTuple() override {
        cur_tup_++;
    }

    bool is_end() const override {
        return cur_tup_ == tuple_num_;
    }

    std::unique_ptr<RmRecord> Next() override {
        char* data = get_tuple(cur_tup_, curr_district_);
        return std::make_unique<RmRecord>(tuple_len_, data);
    }

    Rid &rid() override {
        return _abstract_rid;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    size_t tupleLen() const override {
        return static_cast<size_t>(tuple_len_);
    };

private:
    std::vector<Value> getOrder(char* data) {
        std::vector<Value> values;

        for (auto& col_meta: order_bys_) {
            Value val;
            val.type = col_meta.type;

            char* col_raw = new char[col_meta.len];
            memset(col_raw, 0, col_meta.len);
            memcpy(col_raw, data + col_meta.offset, col_meta.len);

            if (col_meta.type == TYPE_INT) {
                auto int_val = *(int *)(col_raw);
                val.int_val = int_val;
            } else if (col_meta.type == TYPE_FLOAT) {
                auto float_val = *(float *)(col_raw);
                val.float_val = float_val;
            } else if (col_meta.type == TYPE_STRING) {
                auto str_val = std::string(col_raw, col_meta.len);
                str_val.resize(strlen(str_val.c_str()));
                val.str_val = str_val;
            }

            values.push_back(val);
        }

        return values;
    }

    inline CompOp judge_order(char* tuple_left, char* tuple_right) {
        auto order1 = getOrder(tuple_left);
        auto order2 = getOrder(tuple_right);
        for (size_t i = 0; i < order1.size(); i++) {
            if (Value::ValueComp(order1.at(i), order2.at(i), OP_GT)) { return OP_GT; }
            if (Value::ValueComp(order1.at(i), order2.at(i), OP_LT)) { return OP_LT; }
        }
        return OP_EQ;
    }

    inline char* get_tuple(int offset, int district) {
        page_id_t page_no = (offset + district * tuple_num_) / tup_per_page_;
        int page_offset = (offset + district * tuple_num_) % tup_per_page_;
        PageId page_id {
            .fd = tmp_fd_,
            .page_no = page_ids_[page_no]
        };
        return bpm_->fetch_page(page_id)->get_data() + page_offset * tuple_len_;
    }

    inline void modify_tuple(int offset, char* data, int district) {
        int page_offset = (offset + district * tuple_num_) % tup_per_page_;
        page_id_t page_no = (offset + district * tuple_num_) / tup_per_page_;
        PageId page_id {
            .fd = tmp_fd_,
            .page_no = page_ids_[page_no]
        };
        Page* page = bpm_->fetch_page(page_id);
        memcpy(page->get_data() + page_offset * tuple_len_, data, tuple_len_);
        bpm_->unpin_page(page_id, true);
    }

    inline void insert_tuple(int offset, char* data, int district) {
        int page_offset = (offset + district * tuple_num_) % tup_per_page_;
        PageId page_id {.fd = tmp_fd_, .page_no = 0};
        if (page_offset == 0) {
            bpm_->new_page(&page_id);
            page_ids_.push_back(page_id.page_no);
        } else {
            page_id_t page_no = (offset + district * tuple_num_) / tup_per_page_;
            page_id.page_no = page_ids_[page_no];
        }
        Page* page = bpm_->fetch_page(page_id);
        memcpy(page->get_data() + page_offset * tuple_len_, data, tuple_len_);
        bpm_->unpin_page(page_id, true);
    }

    inline int ceil_int(int num1, int num2) {
        return (num1 + num2 - 1) / num2;
    }

    inline std::string get_tmp_name(std::vector<ColMeta> order_bys) {
        std::string tmp_name;
        for (auto& order_by : order_bys_) {
            tmp_name += (order_by.tab_name + "_" + order_by.name);
        }
        return tmp_name;
    }
};