package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient
import org.apache.hc.client5.http.impl.async.HttpAsyncClients
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.reactor.IOReactorConfig
import org.apache.hc.core5.util.Timeout
import org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500
import org.eclipse.jetty.http.HttpStatus.REQUEST_TIMEOUT_408
import org.eclipse.jetty.http.HttpStatus.TOO_MANY_REQUESTS_429
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val hedgeDelaysMs = listOf(0L)

    private val connectTimeoutMs = 500L
    private val readTimeoutMs = 300L

    private val requestConfig: RequestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(Timeout.ofMilliseconds(connectTimeoutMs))
        .setResponseTimeout(Timeout.ofMilliseconds(readTimeoutMs))
        .build()

    private val connectionConfig: ConnectionConfig = ConnectionConfig.custom()
        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeoutMs))
        .build()

    private val client: CloseableHttpAsyncClient =
        HttpAsyncClients.customHttp2()
            .setDefaultConnectionConfig(connectionConfig)
            .setDefaultRequestConfig(requestConfig)
            .build()

    private val rateLimiter = RateLimiterRegistry.of(
        RateLimiterConfig.custom()
            .limitForPeriod(maxOf(1, properties.rateLimitPerSec))
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(Duration.ZERO)
            .build()
    ).rateLimiter("payment-ext-$accountName")

    private val bulkhead = BulkheadRegistry.of(
        BulkheadConfig.custom()
            .maxConcurrentCalls(maxOf(1, properties.parallelRequests))
            .maxWaitDuration(Duration.ZERO)
            .build()
    ).bulkhead("payment-ext-$accountName")

    private val circuitBreaker = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(5) // seconds
            .minimumNumberOfCalls(20)
            .failureRateThreshold(40f)
            .slowCallRateThreshold(60f)
            .slowCallDurationThreshold(Duration.ofMillis(readTimeoutMs))
            .waitDurationInOpenState(Duration.ofSeconds(3))
            .permittedNumberOfCallsInHalfOpenState(3)
            .recordException { e ->
                e !is java.util.concurrent.CancellationException
            }
            .build()
    ).circuitBreaker("payment-ext-$accountName")

    private val scheduledExecutor = ScheduledThreadPoolExecutor(
        16,
        NamedThreadFactory("payment-scheduled-$accountName")
    )

    private val esExecutor: ExecutorService = Executors.newFixedThreadPool(
        8,
        NamedThreadFactory("payment-es-updates-$accountName")
    )

    private val extTimer = Timer
        .builder("external.payment.duration")
        .description("End-to-end payment duration")
        .tag("accountName", accountName)
        .publishPercentiles(0.5, 0.85, 0.95, 0.99)
        .register(meterRegistry)

    private val activeRequests = AtomicInteger(0)

    init {
        client.start()
        Gauge.builder("payment.active_requests") { activeRequests.get().toDouble() }
            .description("Number of active payment requests")
            .tag("accountName", accountName)
            .register(meterRegistry)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        executeHedgedPaymentAsync(
            paymentId = paymentId,
            amount = amount,
            transactionId = transactionId,
            deadline = deadline
        ).whenComplete { result, throwable ->
            val finalResult = when {
                throwable != null -> PaymentResult(
                    success = false,
                    reason = throwable.message ?: "Hedged payment failed",
                    transactionId = transactionId
                )

                result != null -> result

                else -> PaymentResult(
                    success = false,
                    reason = "Unknown hedged payment result",
                    transactionId = transactionId
                )
            }

            submitEsUpdate(paymentId) {
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        success = finalResult.success,
                        processedAt = now(),
                        transactionId = finalResult.transactionId,
                        reason = finalResult.reason
                    )
                }
            }
        }
    }

    private data class PaymentResult(
        val success: Boolean,
        val reason: String?,
        val transactionId: UUID
    )

    private fun executeHedgedPaymentAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): CompletableFuture<PaymentResult> {
        val resultFuture = CompletableFuture<PaymentResult>()
        val completed = AtomicBoolean(false)
        val failedAttempts = AtomicInteger(0)
        val attempts = CopyOnWriteArrayList<CompletableFuture<PaymentResult>>()

        val remaining = deadline - now()
        if (remaining <= 0L) {
            resultFuture.complete(
                PaymentResult(false, "Deadline exceeded before hedged start", transactionId)
            )
            return resultFuture
        }

        hedgeDelaysMs.forEach { delayMs ->
            scheduledExecutor.schedule({
                if (completed.get() || now() >= deadline) {
                    return@schedule
                }

                val attempt = executeSingleAttemptAsync(
                    paymentId = paymentId,
                    amount = amount,
                    transactionId = transactionId,
                    deadline = deadline
                )

                attempts.add(attempt)

                attempt.whenComplete { result, throwable ->
                    if (completed.get()) {
                        return@whenComplete
                    }

                    val attemptResult = when {
                        throwable != null -> PaymentResult(
                            success = false,
                            reason = throwable.message ?: "Attempt failed",
                            transactionId = transactionId
                        )

                        result != null -> result

                        else -> PaymentResult(
                            success = false,
                            reason = "Empty attempt result",
                            transactionId = transactionId
                        )
                    }

                    if (attemptResult.success) {
                        if (completed.compareAndSet(false, true)) {
                            resultFuture.complete(attemptResult)
                            attempts.forEach { other ->
                                if (other !== attempt) {
                                    other.cancel(true)
                                }
                            }
                        }
                    } else {
                        if (failedAttempts.incrementAndGet() >= hedgeDelaysMs.size &&
                            completed.compareAndSet(false, true)
                        ) {
                            resultFuture.complete(attemptResult)
                        }
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS)
        }

        scheduledExecutor.schedule({
            if (completed.compareAndSet(false, true)) {
                attempts.forEach { it.cancel(true) }
                resultFuture.complete(
                    PaymentResult(false, "Deadline exceeded", transactionId)
                )
            }
        }, remaining, TimeUnit.MILLISECONDS)

        return resultFuture
    }

    private fun executeSingleAttemptAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): CompletableFuture<PaymentResult> {
        val isTimedOut = AtomicBoolean(false)
        val future = CompletableFuture<PaymentResult>()

        if (now() >= deadline) {
            future.complete(
                PaymentResult(false, "Deadline exceeded before send", transactionId)
            )
            return future
        }

        if (!rateLimiter.acquirePermission()) {
            future.complete(PaymentResult(false, "Rate limited", transactionId))
            return future
        }

        if (!bulkhead.tryAcquirePermission()) {
            future.complete(PaymentResult(false, "Bulkhead full", transactionId))
            return future
        }

        if (!circuitBreaker.tryAcquirePermission()) {
            future.complete(PaymentResult(false, "Circuit breaker OPEN", transactionId))
            return future
        }

        val url =
            "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"

        val request = SimpleRequestBuilder.post(url)
            .addHeader("x-idempotency-key", transactionId.toString())
            .setBody("", ContentType.TEXT_PLAIN)
            .build()

        val startAll = now()
        activeRequests.incrementAndGet()

        val apacheFuture = client.execute(request, object : FutureCallback<SimpleHttpResponse> {
            override fun completed(response: SimpleHttpResponse) {
                releaseActiveOnly()
                recordDuration(startAll)
                bulkhead.onComplete()

                if (future.isDone) return

                try {
                    val status = response.code
                    if (status == TOO_MANY_REQUESTS_429 ||
                        status == REQUEST_TIMEOUT_408 ||
                        status >= INTERNAL_SERVER_ERROR_500
                    ) {
                        circuitBreaker.onError(
                            now() - startAll,
                            TimeUnit.MILLISECONDS,
                            RuntimeException("HTTP $status")
                        )
                        future.complete(
                            PaymentResult(false, "HTTP $status", transactionId)
                        )
                        return
                    }

                    val raw = response.bodyText
                    val body = try {
                        mapper.readValue(raw, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        circuitBreaker.onError(now() - startAll, TimeUnit.MILLISECONDS, e)
                        future.complete(
                            PaymentResult(false, "Bad response: ${e.message}", transactionId)
                        )
                        return
                    }

                    if (body.result) {
                        circuitBreaker.onSuccess(now() - startAll, TimeUnit.MILLISECONDS)
                    } else {
                        circuitBreaker.onError(
                            now() - startAll,
                            TimeUnit.MILLISECONDS,
                            RuntimeException(body.message ?: "Business failure")
                        )
                    }
                    future.complete(
                        PaymentResult(
                            success = body.result,
                            reason = body.message,
                            transactionId = transactionId
                        )
                    )
                } catch (e: Exception) {
                    circuitBreaker.onError(now() - startAll, TimeUnit.MILLISECONDS, e)
                    future.complete(
                        PaymentResult(false, e.message ?: "Unknown completion error", transactionId)
                    )
                }
            }

            override fun failed(ex: Exception) {
                releaseActiveOnly()
                recordDuration(startAll)
                bulkhead.onComplete()
                if (future.isDone) {
                    circuitBreaker.releasePermission()
                    return
                }

                circuitBreaker.onError(now() - startAll, TimeUnit.MILLISECONDS, ex)
                future.complete(
                    PaymentResult(false, "Request failed: ${ex.message}", transactionId)
                )
            }

            override fun cancelled() {
                releaseActiveOnly()
                recordDuration(startAll)
                bulkhead.onComplete()
                if (!isTimedOut.get()) {
                    circuitBreaker.releasePermission()
                }
                if (!future.isDone) {
                    future.complete(PaymentResult(false, "Request cancelled", transactionId))
                }
            }
        })

        val timeoutTask = scheduledExecutor.schedule({
            if (!future.isDone) {
                isTimedOut.set(true)
                circuitBreaker.onError(readTimeoutMs, TimeUnit.MILLISECONDS, RuntimeException("Read timeout"))
                apacheFuture.cancel(true)
                future.complete(PaymentResult(false, "Read timeout after ${readTimeoutMs}ms", transactionId))
            }
        }, readTimeoutMs, TimeUnit.MILLISECONDS)

        future.whenComplete { _, _ ->
            timeoutTask.cancel(false)
            if (future.isCancelled && !apacheFuture.isCancelled) {
                apacheFuture.cancel(true)
            }
        }

        return future
    }

    private fun submitEsUpdate(
        paymentId: UUID,
        attempt: Int = 0,
        block: () -> Unit
    ) {
        esExecutor.submit {
            try {
                block()
            } catch (e: IllegalArgumentException) {
                val aggregateMissing = e.message?.contains("do not exist", ignoreCase = true) == true
                if (aggregateMissing && attempt < 5) {
                    val delayMs = when (attempt) {
                        0 -> 5L
                        1 -> 10L
                        2 -> 20L
                        3 -> 40L
                        else -> 80L
                    }
                    scheduledExecutor.schedule(
                        { submitEsUpdate(paymentId, attempt + 1, block) },
                        delayMs,
                        TimeUnit.MILLISECONDS
                    )
                } else {
                    logger.error("[$accountName] ES update failed for payment $paymentId", e)
                }
            } catch (e: Exception) {
                logger.error("[$accountName] ES update failed for payment $paymentId", e)
            }
        }
    }

    private fun releaseActiveOnly() {
        activeRequests.decrementAndGet()
    }

    private fun recordDuration(startAll: Long) {
        extTimer.record(now() - startAll, TimeUnit.MILLISECONDS)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()