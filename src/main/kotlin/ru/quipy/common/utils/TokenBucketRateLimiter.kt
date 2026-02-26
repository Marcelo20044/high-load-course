package ru.quipy.common.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
): RateLimiter {
    private val lock = ReentrantLock()
    private var tokens: Int = 0
    private var lastRefillTimestampNanos: Long = System.nanoTime()

    override fun tick(): Boolean {
        lock.withLock {
            refillTokens()
            if (tokens <= 0) {
                return false
            }
            tokens--
            return true
        }
    }

    private fun refillTokens() {
        val now = System.nanoTime()
        val elapsedNanos = now - lastRefillTimestampNanos
        if (elapsedNanos <= 0L) return

        val windowNanos = timeUnit.toNanos(window)
        if (windowNanos <= 0L) return

        val tokensToAdd = (elapsedNanos * rate / windowNanos).toInt()
        if (tokensToAdd > 0) {
            tokens = minOf(bucketMaxCapacity, tokens + tokensToAdd)
            lastRefillTimestampNanos = now
        }
    }
}