package ru.quipy.payments.logic

import io.micrometer.core.instrument.Tag
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.PaymentMetrics
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var metrics: PaymentMetrics

    private val userSlaMillis: Long = 30_000L

    private val accountRateLimitPerSec: Int = 11

    private val accountParallelRequests: Int = 64

    private val safeQueueCapacity: Int = run {
        val totalBySla = (userSlaMillis / 1000L * accountParallelRequests).toInt()
        (totalBySla - accountParallelRequests).coerceAtLeast(1)
    }

    private val queue = ArrayBlockingQueue<Runnable>(safeQueueCapacity, true)
    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            metrics.incEnqueued(tags = listOf(Tag.of("component", "order_payer")))

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

    @PostConstruct
    fun bindMetrics() {
        metrics.registerExecutorGauges(paymentExecutor, tags = listOf(Tag.of("component", "order_payer")))
    }
}