//
// Created by Ryan on 2024/8/5.
//

#include "disk_scheduler.h"

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_mgr_(disk_manager) {}

DiskScheduler::~DiskScheduler() = default;

void write_page(int fd, page_id_t page_no, const char *offset,int num_bytes,DiskScheduler::DiskWriteRequestQueue *queue, DiskManager *disk_mgr)  {
    disk_mgr->write_page(fd, page_no, offset, PAGE_SIZE);
    auto request = queue->request_queue_.front();
    queue->request_queue_.pop_front();
    if (queue->request_queue_.empty()) {
        memcpy(queue->cache_, request->page_data_, num_bytes);
        queue->cache_valid_ = true;
    } else {
        auto next = queue->request_queue_.front();
        next->granted_ = true;
        queue->cv_.notify_all();
    }
    // std::unique_lock<std::mutex> lck_fd_map(disk_mgr->latch_);
    if(disk_mgr->fd_map_.find(fd) != disk_mgr->fd_map_.end()){
        --disk_mgr->fd_map_[fd];
    }
    // lck_fd_map.unlock();
    disk_mgr->cv_.notify_all();
}

bool DiskScheduler::write_page_schedule(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // std::unique_lock<std::mutex> lck_fd_map(disk_mgr_->latch_);
    if(disk_mgr_->fd_map_.find(fd) == disk_mgr_->fd_map_.end()) {
        disk_mgr_->fd_map_.emplace(fd,1);
    }else {
        ++disk_mgr_->fd_map_[fd];
    }
    // lck_fd_map.unlock();

    PageId page_id = PageId();
    page_id.fd = fd;
    page_id.page_no = page_no;
    std::unique_lock<std::mutex> lck_map(latch_);
    if (disk_write_request_queue_map.find(page_id) == disk_write_request_queue_map.end() ||
        disk_write_request_queue_map[page_id] == nullptr) {
        auto write_request_queue = std::make_shared<DiskWriteRequestQueue>();
        disk_write_request_queue_map[page_id] = write_request_queue;
    }
    auto queue = disk_write_request_queue_map[page_id];
    lck_map.unlock();

    std::unique_lock<std::mutex> lck(queue->latch_);
    auto write_request = std::make_shared<WriteRequest>(offset,num_bytes);
    if (queue->request_queue_.empty()) {
        write_request->granted_ = true;
    }
    queue->request_queue_.emplace_back(write_request);
    queue->cache_valid_ = false;
    queue->cv_.wait(lck, [&]() { return write_request->granted_; });
    char off_set[PAGE_SIZE];
    memcpy(off_set, offset, num_bytes);
    std::thread t(write_page, fd,page_no,off_set,num_bytes,queue.get(),disk_mgr_);
    t.detach();
    return true;
}

bool DiskScheduler::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    std::unique_lock<std::mutex> lck_map(latch_);
    PageId page_id = PageId();
    page_id.fd = fd;
    page_id.page_no = page_no;
    if (disk_write_request_queue_map.find(page_id) == disk_write_request_queue_map.end() ||
        disk_write_request_queue_map[page_id] == nullptr) {
        auto write_request_queue = std::make_shared<DiskWriteRequestQueue>();
        disk_write_request_queue_map[page_id] = write_request_queue;
    }
    auto queue = disk_write_request_queue_map[page_id];
    lck_map.unlock();
    std::unique_lock<std::mutex> lck(queue->latch_);
    if (!queue->request_queue_.empty()) {
        memcpy(offset, queue->request_queue_.back()->page_data_, num_bytes);
        return true;
    }
    if (queue->cache_valid_) {
        memcpy(offset, queue->cache_, num_bytes);
    } else {
        disk_mgr_->read_page(fd, page_no, offset, PAGE_SIZE);
        memcpy(queue->cache_, offset, PAGE_SIZE);
        queue->cache_valid_ = true;
    }
    return true;
}




