package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
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
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.DiscardOldestPolicy()
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

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = now()
        // Ограничиваем длину очереди задач — если ожидающих задач больше 5000,
        // сразу отвечаем 429 с небольшим Retry-After, чтобы не копить долгие хвосты.
        if (paymentExecutor.queue.size > 5000) {
            rejectedQueue.increment()
            throw TooManyRequestsException(30)
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

        paymentExecutor.submit(task)

        return createdAt
    }
}

class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many requests")