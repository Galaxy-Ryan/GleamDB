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

#include <mutex>
#include <vector>
#include <iostream>
#include "log_defs.h"
#include "common/config.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    begin,
    commit,
    ABORT,
    Checkpoint
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "CHECKPOINT"
};

class LogRecord {
public:
    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const LogType*>(src);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
    }
    virtual void format_log() {
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << LogTypeStr[log_type_] << "  lsn:" << lsn_ << "  log_tid:" << log_tid_ << "  prev_lsn:" << prev_lsn_;
        outfile.close();
    };
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::begin;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;
    }
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);   
    }
    virtual void format_print() override {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "\n";
        outfile.close();
    }
};

/**
 * TODO: commit操作的日志记录
*/
class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord() {
        log_type_ = LogType::commit;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }

    CommitLogRecord(txn_id_t txn_id)
            : CommitLogRecord() {
        log_tid_ = txn_id;
    }

    // 把commit日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        // Commit log record has no additional data beyond the header
    }

    // 从src中反序列化出一条Commit日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        // Commit log record has no additional data beyond the header
    }

    void format_print() override {
        printf("commit record\n");
        LogRecord::format_print();
    }
    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "\n";
        outfile.close();
    }
};

/**
 * TODO: abort操作的日志记录
*/
class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord() {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }

    AbortLogRecord(txn_id_t txn_id)
            : AbortLogRecord() {
        log_tid_ = txn_id;
    }

    // 把abort日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        // Abort log record has no additional data beyond the header
    }

    // 从src中反序列化出一条Abort日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        // Abort log record has no additional data beyond the header
    }

    virtual void format_print() override {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "\n";
        outfile.close();
    }
};

class CkpLogRecord : public LogRecord {
public:
    CkpLogRecord() {
        log_type_ = LogType::Checkpoint;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    void format_log() override {
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "CHECKPOINT\n";
        outfile.close();
    }
};

class InsertLogRecord: public LogRecord {
public:
    InsertLogRecord() {
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    InsertLogRecord(txn_id_t txn_id, lsn_t prev_lsn, RmRecord& insert_value, Rid& rid, std::string table_name,
                    int first_free_page_no, int num_page)
        : InsertLogRecord() {
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;

        insert_value_ = insert_value;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value_.size;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);

        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(int);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(int);
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &insert_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value_.data, insert_value_.size);
        offset += insert_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset += table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);  
        insert_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + insert_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset += table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ = *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        printf("insert record\n");
        LogRecord::format_print();
        std::string tab_name(table_name_, table_name_size_);
        printf("insert_value: %s\n", insert_value_.data);
        printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", tab_name.c_str());
    }
    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        std::string tab_name(table_name_, table_name_size_);
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "  tab:" << tab_name << "  rid:("<< rid_.page_no << "," << rid_.slot_no << ")" << "\n";
        outfile.close();
    }

    RmRecord insert_value_;     // 插入的记录
    Rid rid_;                   // 记录插入的位置
    char* table_name_;          // 插入记录的表名称
    size_t table_name_size_;    // 表名称的大小
    int first_free_page_no_;
    int num_pages_;
};

/**
 * TODO: delete操作的日志记录
*/
class DeleteLogRecord: public LogRecord {
public:
    DeleteLogRecord() {
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }

    DeleteLogRecord(txn_id_t txn_id, lsn_t prev_lsn, RmRecord& delete_value, Rid& rid, std::string table_name,
                    int first_free_page_no, int num_page)
            : DeleteLogRecord() {
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;

        delete_value_ = delete_value;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += delete_value_.size;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);

        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(int);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(int);
    }

    // 把delete日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &delete_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, delete_value_.data, delete_value_.size);
        offset += delete_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset += table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
    }

    // 从src中反序列化出一条Delete日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        delete_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + delete_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset += table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ = *reinterpret_cast<const int*>(src + offset);
    }

    void format_print() override {
        printf("delete record\n");
        LogRecord::format_print();
        std::string tab_name(table_name_, table_name_size_);
        printf("delete_value: %s\n", delete_value_.data);
        printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", tab_name.c_str());
    }

    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        std::string tab_name(table_name_, table_name_size_);
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "  tab:" << tab_name << "  rid:("<< rid_.page_no << "," << rid_.slot_no << ")" << "\n";
        outfile.close();
    }

    RmRecord delete_value_;     // 删除的记录
    Rid rid_;                   // 记录删除的位置
    char* table_name_;          // 删除记录的表名称
    size_t table_name_size_;    // 表名称的大小
    int first_free_page_no_;
    int num_pages_;
};

/**
 * TODO: update操作的日志记录
*/
class UpdateLogRecord: public LogRecord {
public:
    UpdateLogRecord() {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }

    UpdateLogRecord(txn_id_t txn_id, lsn_t prev_lsn, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name,
                    int first_free_page_no, int num_page)
            : UpdateLogRecord() {
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;

        old_value_ = old_value;
        new_value_ = new_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int) * 2;
        log_tot_len_ += old_value_.size + new_value_.size;
        log_tot_len_ += sizeof(Rid);

        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(int);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(int);
    }

    // 把update日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &old_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_value_.data, old_value_.size);
        offset += old_value_.size;
        memcpy(dest + offset, &new_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, new_value_.data, new_value_.size);
        offset += new_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset += table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
    }

    // 从src中反序列化出一条Update日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        old_value_.Deserialize(src + offset);
        offset += sizeof(int) + old_value_.size;
        new_value_.Deserialize(src + offset);
        offset += sizeof(int) + new_value_.size;
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset += table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ = *reinterpret_cast<const int*>(src + offset);
    }

    void format_print() override {
        printf("update record\n");
        LogRecord::format_print();
        std::string tab_name(table_name_, table_name_size_);
        printf("old_value: %s\n", old_value_.data);
        printf("new_value: %s\n", new_value_.data);
        printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", tab_name.c_str());
    }

    virtual void format_log() override {
        LogRecord::format_log();
        std::fstream outfile;
        outfile.open("log.format", std::ios::out | std::ios::app);
        outfile << "  tab:" << table_name_ << "  rid:("<< rid_.page_no << "," << rid_.slot_no << ")" << "\n";
        outfile.close();
    }

    RmRecord old_value_;        // 更新前的记录
    RmRecord new_value_;        // 更新后的记录
    Rid rid_;                   // 记录更新的位置
    char* table_name_;          // 更新记录的表名称
    size_t table_name_size_;    // 表名称的大小
    int first_free_page_no_;
    int num_pages_;
};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    bool is_full(int append_size) {
        if(offset_ + append_size > LOG_BUFFER_SIZE)
            return true;
        return false;
    }

    char buffer_[LOG_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
public:
    LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager; }
    
    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();

    LogBuffer* get_log_buffer() { return &log_buffer_; }
    lsn_t get_lsn() { return global_lsn_; }
    lsn_t get_persist_lsn() { return persist_lsn_; }

private:    
    std::atomic<lsn_t> global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex latch_;                  // 用于对log_buffer_的互斥访问
    LogBuffer log_buffer_;              // 日志缓冲区
    lsn_t persist_lsn_;                 // 记录已经持久化到磁盘中的最后一条日志的日志号
    DiskManager* disk_manager_;
}; 
