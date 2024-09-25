//
// Created by Ryan on 2024/8/5.
//

#pragma once

#include <list>
#include "thread_pool.h"
#include "page.h"
#include "disk_manager.h"

//TODO 引入线程池(optimize)
class DiskScheduler {
public:
    class WriteRequest {
    public:
        explicit WriteRequest(char *page_data,int num_bytes) {
            memcpy(page_data_, page_data, num_bytes);
        }

        char page_data_[PAGE_SIZE] = {};
        bool granted_{false};          // 该写请求是否已经被赋予
    };

    class DiskWriteRequestQueue {
    public:
        std::list<std::shared_ptr<WriteRequest>> request_queue_;
        std::condition_variable cv_;
        /** coordination */
        std::mutex latch_;
        char cache_[PAGE_SIZE] = {};
        bool cache_valid_{false};
    };

    explicit DiskScheduler(DiskManager *disk_manager);

    ~DiskScheduler();

    bool write_page_schedule(int fd, page_id_t page_no, char *offset, int num_bytes);

    bool read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

private:
    DiskManager *disk_mgr_;
    // ThreadPool thread_pool_;

    std::mutex latch_;
    std::unordered_map<PageId, std::shared_ptr<DiskWriteRequestQueue>> disk_write_request_queue_map;
};
