package ru.quipy.payments.logic

import org.slf4j.LoggerFactory

data class ExecutorTask(
    private val block: () -> Unit
) : Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(ExecutorTask::class.java)
    }

    override fun run() {
        try {
            block()
        } catch (e: Exception) {
            log.error("ExecutorTask failed unexpectedly: $this", e)
        }
    }
}
