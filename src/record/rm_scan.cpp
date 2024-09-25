/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    fd_ = file_handle_->fd_;
    num_pages_ = file_handle_->file_hdr_.num_pages;
    num_records_ = file_handle_->file_hdr_.num_records_per_page;

    for (int i = 1; i < num_pages_; i++) {
        auto page_handle = file_handle_->fetch_page_handle(i);
        auto j = Bitmap::next_bit(true, page_handle.bitmap, num_records_, -1);
        file_handle_->buffer_pool_manager_->unpin_page({fd_, i}, false);

        if (j != num_records_) {
            rid_ = {i, j};
            break;
        }

        if (i == num_pages_ - 1) {
            rid_ = {i, j};
        }

        rid_.page_no++;
        rid_.slot_no = -1;
    }

    for (int i = num_pages_ - 1; i > 0; i--) {
        auto page_handle = file_handle_->fetch_page_handle(i);
        auto j = Bitmap::next_bit(true, page_handle.bitmap, num_records_, -1);
        file_handle_->buffer_pool_manager_->unpin_page({fd_, i}, false);

        if (j != num_records_) {
            end_page_no_ = i;
            break;
        }

        if (i == 1) {
            end_page_no_ = INVALID_PAGE_ID;
        }
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    for (int i = rid_.page_no; i <= end_page_no_; i++) {
        auto page_handle = file_handle_->fetch_page_handle(i);
        int next_bit = Bitmap::next_bit(true, page_handle.bitmap, num_records_, rid_.slot_no);
        file_handle_->buffer_pool_manager_->unpin_page({fd_, i}, false);

        if (next_bit != num_records_) {
            rid_ = {i, next_bit};
            break;
        }

        rid_.page_no++;
        rid_.slot_no = -1;
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    if (end_page_no_ == INVALID_PAGE_ID) {
        return true;
    }

    if (rid_.page_no > end_page_no_) {
        return true;
    }

    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}