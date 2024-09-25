/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <queue>
#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    int offset = 0;
    int log_file_size = disk_manager_->get_file_size(LOG_FILE_NAME);

    char* log_data = new char[LOG_BUFFER_SIZE];
    char* log_header = new char[LOG_HEADER_SIZE];

    std::set<txn_id_t> aborted_tids;

    while (offset < log_file_size) {
        disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset);
        int log_tot_len = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);

        LogType log_type = *(LogType*)(log_header);
        lsn_t lsn = *(lsn_t*)(log_header + OFFSET_LSN);
        txn_id_t log_tid = *(txn_id_t*)(log_header + OFFSET_LOG_TID);
        log_offsets_.insert(std::make_pair(lsn, offset));
        last_lsn_ = lsn;

        if (log_type == begin) {
            disk_manager_->read_log(log_data, log_tot_len, offset);
            BeginLogRecord record;
            record.deserialize(log_data);
            ATT[log_tid] = lsn;
            record.format_log();
        }

        if (log_type == commit) {
            disk_manager_->read_log(log_data, log_tot_len, offset);
            CommitLogRecord record;
            ATT.erase(log_tid);
            record.deserialize(log_data);
            record.format_log();
        }

        if (log_type == ABORT) {
            disk_manager_->read_log(log_data, log_tot_len, offset);
            AbortLogRecord record;
            aborted_tids.insert(log_tid);
            record.deserialize(log_data);
            record.format_log();
        }

        if (log_type == UPDATE) {
            UpdateLogRecord record;
            disk_manager_->read_log(log_data, log_tot_len, offset);
            record.deserialize(log_data);
            record.format_log();

            PageId page_id;
            auto tab_name = std::string(record.table_name_, record.table_name_size_);
            page_id.fd = sm_manager_->fhs_.at(tab_name)->GetFd();
            page_id.page_no = record.rid_.page_no;

            if (DPT.find(page_id) == DPT.end()) {
                DPT.insert(std::make_pair(page_id, lsn));
            }
            ATT[log_tid] = lsn;
        }

        if (log_type == INSERT) {
            InsertLogRecord record;
            disk_manager_->read_log(log_data, log_tot_len, offset);
            record.deserialize(log_data);
            record.format_log();

            PageId page_id;
            auto tab_name = std::string(record.table_name_, record.table_name_size_);
            page_id.fd = sm_manager_->fhs_.at(tab_name)->GetFd();
            page_id.page_no = record.rid_.page_no;

            if (DPT.find(page_id) == DPT.end()) {
                DPT.insert(std::make_pair(page_id, lsn));
            }
            ATT[log_tid] = lsn;
        }

        if (log_type == DELETE) {
            DeleteLogRecord record;
            disk_manager_->read_log(log_data, log_tot_len, offset);
            record.deserialize(log_data);
            record.format_log();

            PageId page_id;
            auto tab_name = std::string(record.table_name_, record.table_name_size_);
            page_id.fd = sm_manager_->fhs_.at(tab_name)->GetFd();
            page_id.page_no = record.rid_.page_no;

            if (DPT.find(page_id) == DPT.end()) {
                DPT.insert(std::make_pair(page_id, lsn));
            }
            ATT[log_tid] = lsn;
        }

        if (log_type == Checkpoint) {
            CkpLogRecord record;
            disk_manager_->read_log(log_data, log_tot_len, offset);
            record.deserialize(log_data);
            record.format_log();
            DPT.clear();
            for (auto tid: aborted_tids) {
                ATT.erase(tid);
            }
            aborted_tids.clear();
        }

        offset += log_tot_len;
    }
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    char* log_data = new char[LOG_BUFFER_SIZE];
    char* log_header = new char[LOG_HEADER_SIZE];

    if (DPT.empty()) { return; }

//    for (auto& dpt: DPT) {
//        auto page = buffer_pool_manager_->fetch_page(dpt.first);
//        lsn_t dirty_lsn = page->get_page_lsn();
//        assert(dirty_lsn >= dpt.second);
//        dpt.second = page->get_page_lsn();
//        buffer_pool_manager_->unpin_page(dpt.first, false);
//    }

    auto it = DPT.begin();
    lsn_t first_lsn = it->second;
    for (auto& dpt: DPT) {
        first_lsn = std::min(dpt.second, first_lsn);
    }

    for (int lsn = first_lsn; lsn <= last_lsn_; lsn++) {
        int offset = log_offsets_[lsn];
        disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset);
        LogType log_type = *(LogType*)(log_header);

        switch (log_type) {
            case INSERT:
            {
                InsertLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_[table_name].get();
                PageId page_id {.fd = fh->GetFd(), .page_no = record.rid_.page_no};
                if ( DPT.find(page_id) == DPT.end()) {
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                fh->insert_redo(record);

                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            case DELETE:
            {
                DeleteLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_[table_name].get();
                PageId page_id {.fd = fh->GetFd(), .page_no = record.rid_.page_no};
                if ( DPT.find(page_id) == DPT.end()) {
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                fh->delete_redo(record);

                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            case UPDATE:
            {
                UpdateLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_[table_name].get();
                PageId page_id {.fd = fh->GetFd(), .page_no = record.rid_.page_no};
                if ( DPT.find(page_id) == DPT.end()) {
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                fh->update_redo(record);

                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            default:
                break;
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    char* log_data = new char[LOG_BUFFER_SIZE];
    char* log_header = new char[LOG_HEADER_SIZE];

    std::priority_queue<lsn_t> lsn_queue;

    for (auto& att: ATT) {
        lsn_queue.push(att.second);
    }

    while (!lsn_queue.empty()) {
        auto lsn = lsn_queue.top();
        if (lsn == -1) { return; }

        int offset = log_offsets_[lsn];
        disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset);
        LogType log_type = *(LogType*)(log_header);

        switch (log_type) {
            case INSERT:
            {
                InsertLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->insert_undo(record.rid_);

                lsn_queue.pop();
                lsn_queue.push(record.prev_lsn_);
                break;
            }

            case UPDATE: {
                UpdateLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->update_undo(record.rid_, record.old_value_.data);

                lsn_queue.pop();
                lsn_queue.push(record.prev_lsn_);
                break;
            }

            case DELETE: {
                DeleteLogRecord record;
                int log_size = *(uint32_t*)(log_header + OFFSET_LOG_TOT_LEN);
                disk_manager_->read_log(log_data, log_size, offset);
                record.deserialize(log_data);

                std::string table_name(record.table_name_, record.table_name_size_);
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->delete_undo(record.rid_, record.delete_value_.data);

                lsn_queue.pop();
                lsn_queue.push(record.prev_lsn_);
                break;
            }

            case begin: {
                lsn_queue.pop();
                break;
            }

            case commit: case ABORT: {
                break;
            }
        }
    }

    auto indexes = sm_manager_->db_.get_all_indexes();

    for (auto& index: indexes) {
        std::vector<std::string> col_names;
        for (auto& col: index.cols) {
            col_names.push_back(col.name);
        }
        sm_manager_->drop_index(index.tab_name, col_names, nullptr);
        sm_manager_->create_index(index.tab_name, col_names, nullptr);
    }
}