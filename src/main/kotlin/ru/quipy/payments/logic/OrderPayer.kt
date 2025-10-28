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
import java.time.Duration
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
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
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(128),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val acceptedCounter: Counter = Counter
        .builder("payments.accepted")
        .register(registry)

    private val rejectedExpired = registry.counter("payments.rejected", "code", "429", "reason", "expired")
    private val rejectedDeadline = registry.counter("payments.rejected", "code", "429", "reason", "deadline_budget")
    private val rejectedLimiter  = registry.counter("payments.rejected", "code", "429", "reason", "limiter_throttle")
    private val rejectedQueue    = registry.counter("payments.rejected", "code", "429", "reason", "queue_overflow")


    init {
        Gauge.builder("waiting.queue.size") { paymentExecutor.queue.size.toDouble() }
            .description("Tasks waiting in payment submission executor queue")
            .register(registry)
    }

    private val ingressRate = 10
    private val ingressLimiter = CompositeRateLimiter(
        TokenBucketRateLimiter(
            rate = ingressRate,
            bucketMaxCapacity = ingressRate * 4,
            window = 1,
            timeUnit = TimeUnit.SECONDS
        ), LeakingBucketRateLimiter(
            rate = ingressRate.toLong(),
            window = Duration.ofSeconds(1),
            bucketSize = 16
        )
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
        val avgProcMs = 700L
        val jitterMs = 200L
        val safety = avgProcMs + jitterMs
        if (qWaitMs + safety >= timeBudgetMs) {
            rejectedDeadline.increment()
            val backoffMs = (1000.0 / ingressRate).toLong().coerceAtLeast(100)
            throw TooManyRequestsException(backoffMs)
        }

        if (!ingressLimiter.tick()) {
            rejectedLimiter.increment()
            val backoffMs = 120L
            throw TooManyRequestsException(backoffMs)
        }

        if (paymentExecutor.queue.remainingCapacity() == 0) {
            rejectedQueue.increment()
            throw TooManyRequestsException(200)
        }

        acceptedCounter.increment()

        paymentExecutor.submit {
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
        return createdAt
    }
}

class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many requests")