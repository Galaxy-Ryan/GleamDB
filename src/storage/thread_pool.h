//
// Created by Ryan on 2024/8/6.
//

#ifndef RMDB_THREAD_POOL_H
#define RMDB_THREAD_POOL_H

#pragma once
#include<queue>
#include<mutex>
#include<functional>
#include<vector>
#include<thread>
#include<condition_variable>
#include<future>

template<typename T>
class SafeQueue {
private:
    std::queue<T> m_queue;
    std::mutex m_mutex;

public:
    SafeQueue();

    SafeQueue(SafeQueue &&other);

    ~SafeQueue();

    bool empty() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

    int size() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

    void enqueue(T &t) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.emplace(&t);
    }

    bool dequeue(T &t) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_queue.empty())
            return false;
        t = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }
};

class ThreadPool {
private:
    class ThreadWorker {
    private:
        int m_id;
        ThreadPool *m_pool;

    public:
        ThreadWorker(const int id, ThreadPool *pool) : m_id(id), m_pool(pool) {}

        void operator()()//重载操作
        {
            std::function<void()> func;
            bool dequeued;
            if (!m_pool->m_shutdown) {
                std::unique_lock<std::mutex> lock(m_pool->m_conditional_mutex);
                if (m_pool->m_queue.empty()) {
                    m_pool->m_conditional_lock.wait(lock);
                }
                dequeued = m_pool->m_queue.dequeue(func);
            }
            if (dequeued) {
                func();
            }
        }
    };

    bool m_shutdown;
    std::vector<std::thread> m_threads;
    SafeQueue<std::function<void()>> m_queue;
    std::mutex m_conditional_mutex;
    std::condition_variable m_conditional_lock;

public:
    ThreadPool(const int n_threads = 4) : m_threads(std::vector<std::thread>(n_threads)), m_shutdown(false) {}

    ThreadPool(const ThreadPool &) = delete;

    ThreadPool(ThreadPool &&) = delete;

    ThreadPool &operator=(const ThreadPool &) = delete;

    ThreadPool &&operator=(ThreadPool &&) = delete;

    void init()//初始化分配线程
    {
        for (int i = 0; i < m_threads.size(); i++) {
            m_threads.at(i) = std::thread(ThreadWorker(i, this));
        }
    }

    void shutdown()//关闭线程
    {
        m_shutdown = true;
        m_conditional_lock.notify_all();
        for (int i = 0; i < m_threads.size(); i++) {
            if (m_threads.at(i).joinable()) {
                m_threads.at(i).join();
            }
        }
    }

    template<typename F, typename... Args>
    auto submit(F &&f, Args &&...args) -> std::future<decltype(f(args...))> {

        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f),
                                                               std::forward<Args>(args)...);//forward为完美转发


        auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);

        std::function<void()> task = [task_ptr]() {
            (*task_ptr)();
        };
        m_queue.enqueue(task);

        m_conditional_lock.notify_one();

        return task_ptr->get_future();
    }
};


#endif //RMDB_THREAD_POOL_H
