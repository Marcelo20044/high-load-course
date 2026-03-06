package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import ru.quipy.common.utils.TokenBucketRateLimiter
import java.util.concurrent.RejectedExecutionException
import kotlin.math.ceil
import kotlin.math.min

@Service
class OrderPayer(registry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        128,
        256,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(5000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val acceptedCounter: Counter = Counter
        .builder("payments.accepted")
        .register(registry)

    private val rejectedExpired = registry.counter("payments.rejected", "code", "429", "reason", "expired")
    private val rejectedDeadline = registry.counter("payments.rejected", "code", "429", "reason", "deadline_budget")
    private val rejectedLimiter = registry.counter("payments.rejected", "code", "429", "reason", "limiter_throttle")
    private val rejectedQueue = registry.counter("payments.rejected", "code", "429", "reason", "queue_overflow")


    init {
        Gauge.builder("waiting.queue.size") { paymentExecutor.queue.size.toDouble() }
            .description("Tasks waiting in payment submission executor queue")
            .register(registry)
    }

    private val ingressRate = 1100
    private val limiter = TokenBucketRateLimiter(
        rate = ingressRate,
        bucketMaxCapacity = ingressRate * 10,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = now()
        val timeBudgetMs = deadline - createdAt
        if (timeBudgetMs <= 0L) {
            rejectedExpired.increment()
            throw TooManyRequestsException(100)
        }

        val qSize = paymentExecutor.queue.size + 1
        val qWaitMs = ((qSize.toDouble() / ingressRate) * 1000).toLong()
        val avgProcMs = 1000L
        val jitterMs = 300L
        val safety = avgProcMs + jitterMs
        if (qWaitMs + safety >= timeBudgetMs) {
            rejectedDeadline.increment()
            val retryBase = ceil(1000.0 / ingressRate).toLong()
            val backoffMs = (retryBase + min(qWaitMs, 2000)).coerceIn(50, 3000)
            throw TooManyRequestsException(backoffMs)
        }

        if (!limiter.tick()) {
            rejectedLimiter.increment()
            val retryBase = ceil(1000.0 / ingressRate).toLong()
            val qWaitMs = ((paymentExecutor.queue.size.toDouble() / ingressRate) * 1000).toLong()
            val backoffMs = (retryBase + min(qWaitMs, 2000)).coerceIn(50, 3000)
            throw TooManyRequestsException(backoffMs)
        }

        acceptedCounter.increment()

        val task = ExecutorTask {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        try {
            paymentExecutor.submit(task)
        } catch (ex: RejectedExecutionException) {
            rejectedQueue.increment()
            val qSizeAfterReject = paymentExecutor.queue.size
            val backoffMs = (5 * ceil(1000.0 / ingressRate)).toLong()
            logger.error(
                "paymentExecutor rejected paymentId={}, queueSize={}, activeThreads={}",
                paymentId,
                qSizeAfterReject,
                paymentExecutor.activeCount,
                ex
            )
            throw TooManyRequestsException(backoffMs)
        }

        return createdAt
    }
}

class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many requests")