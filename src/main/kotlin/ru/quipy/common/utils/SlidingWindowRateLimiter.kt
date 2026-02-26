package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {

    private val lock = ReentrantLock()
    private val timestamps = ArrayDeque<Long>()

    override fun tick(): Boolean {
        val now = System.currentTimeMillis()
        val windowStart = now - window.toMillis()

        lock.withLock {
            // удалить старые события за пределами окна
            while (true) {
                val head = timestamps.peekFirst() ?: break
                if (head >= windowStart) {
                    break
                }
                timestamps.removeFirst()
            }

            if (timestamps.size.toLong() >= rate) {
                return false
            }

            timestamps.addLast(now)
            return true
        }
    }

    fun tickBlocking(sleepInterval: Duration) {
        while (!tick()) {
            Thread.sleep(sleepInterval.toMillis())
        }
    }

}