/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    // 计算日志记录的大小
    int log_record_size = log_record->log_tot_len_;

    // 检查日志缓冲区是否有足够的空间
    if (log_buffer_.is_full(log_record_size)) {
        // 没有足够的空间，先刷到磁盘
        flush_log_to_disk();
    }

    std::unique_lock<std::mutex> lock(latch_); // 确保线程安全

    // 设置日志记录的LSN

    log_record->lsn_ = global_lsn_++;

    // 将日志记录复制到日志缓冲区中
    char *dest = new char[log_record_size];
    log_record->serialize(dest);
    std::memcpy(log_buffer_.buffer_+log_buffer_.offset_, dest, log_record_size);

    // 更新缓冲区大小和下一个LSN
    log_buffer_.offset_ += log_record_size;

    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    std::unique_lock<std::mutex> lock(latch_); // 确保线程安全

    if (log_buffer_.offset_ == 0) {
        // 如果缓冲区为空，则不需要刷到磁盘
        return;
    }

    // 将日志缓冲区的内容写入到磁盘文件中
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);

    // 清空日志缓冲区
    log_buffer_.offset_ = 0;
    memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));

    // 更新持久化的LSN
    persist_lsn_ = global_lsn_;
}
