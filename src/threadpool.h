#pragma once

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>



class threadpool {
    public:

        // Constructor creates a threadpool with the specified number of threads
        threadpool(unsigned num_threads): {
            for (unsigned i = 0; i < num_threads; ++i) {
                threads.push_back(std::thread(&threadpool::threadLoop, this));
            }
        }

        // Destructor joins all threads
        ~threadpool(){
            // set the stop flag to true
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                stopFlag = true;
            }

            // wake up all threads 
            waitCondition.notify_all();

            // Wait for all threads to finish
            for (auto& thread : threads) {
                thread.join();
            }
            threads.clear();
        }

        // Add a task to the queue
        void addJob(const std::function<void()>& job){
            std::unique_lock<std::mutex> lock(queueMutex);
            jobsQueue.push(job);
            waitCondition.notify_one();
        }
        
        // check if all jobs are done before calling destructor
        void busy(){
            bool busy;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                busy = jobsQueue.empty();
            }
            return busy;
        }	
    private:
        std::vector<std::thread> threads;
        std::queue<std::function<void()>> jobsQueue;
        std::mutex queueMutex;
        std::condition_variable waitCondition;
        bool stopFlag;

        // thread will run this function to get jobs from queue
        void threadLoop()
        {
            while (true)
            {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    waitCondition.wait(lock, [this]
                                       { return !jobsQueue.empty() || stopFlag; });
                    if (stopFlag)
                    {
                        return;
                    }
                    job = jobsQueue.front();
                    jobsQueue.pop();
                }
                job();
            }
        }
};

#endif