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
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import ru.quipy.common.utils.TokenBucketRateLimiter
import java.util.concurrent.RejectedExecutionException
import kotlin.coroutines.cancellation.CancellationException
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

    private val ingressRate = 5000
    private val limiter = TokenBucketRateLimiter(
        rate = ingressRate,
        bucketMaxCapacity = ingressRate * 2,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    private val paymentScope = CoroutineScope(
        Dispatchers.IO.limitedParallelism(256) +
                SupervisorJob() +
                CoroutineName("payment-scope")
    )

    private val inFlightSemaphore = Semaphore(5000)

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = now()
        val timeBudgetMs = deadline - createdAt

        if (timeBudgetMs <= 0L) {
            rejectedExpired.increment()
            throw TooManyRequestsException(100)
        }

        if (!limiter.tick()) {
            rejectedLimiter.increment()
            throw TooManyRequestsException(50)
        }

        // tryAcquire() — не блокирует поток: если слотов нет, сразу отдаём 429
        if (!inFlightSemaphore.tryAcquire()) {
            rejectedQueue.increment()
            throw TooManyRequestsException(100)
        }

        acceptedCounter.increment()

        // launch: fire-and-forget внутри скоупа.
        // SupervisorJob гарантирует, что падение одной корутины
        // не убивает весь скоуп и остальные платежи.
        paymentScope.launch {
            // Сначала проверяем deadline ДО любых IO-операций
            val remaining = deadline - now()
            if (remaining <= 0L) {
                rejectedDeadline.increment()
                inFlightSemaphore.release()
                return@launch
            }

            // ES.create запускаем в отдельной корутине — не блокирует payment-путь
            launch(Dispatchers.IO) {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                } catch (e: Exception) {
                    logger.error("ES create failed for payment $paymentId", e)
                }
            }

            // Сразу идём к платежу, пока deadline ещё актуален
            try {
                withTimeout(remaining - 50) { // -50ms запас на сеть
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                }
            } catch (e: TimeoutCancellationException) {
                logger.warn("Payment $paymentId deadline exceeded during HTTP call")
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Payment $paymentId failed", e)
            } finally {
                inFlightSemaphore.release()
            }
        }

        return createdAt
    }
}

class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many requests")