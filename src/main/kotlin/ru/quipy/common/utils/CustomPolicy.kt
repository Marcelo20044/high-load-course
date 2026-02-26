package ru.quipy.common.utils

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor

class CallerBlockingRejectedExecutionHandler(
) : RejectedExecutionHandler {
    override fun rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
        if (!executor.isShutdown) {
            val queue = executor.queue
            val offered = queue.offer(r)
            if (!offered) {
                throw RejectedExecutionException("Queue is full, task rejected")
            }
        } else {
            throw RejectedExecutionException("Executor has been shut down")
        }
    }
}