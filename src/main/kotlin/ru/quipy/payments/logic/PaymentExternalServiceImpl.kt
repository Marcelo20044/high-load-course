package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.Gauge
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient
import org.apache.hc.client5.http.impl.async.HttpAsyncClients
import org.apache.hc.core5.concurrent.FutureCallback
import org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500
import org.eclipse.jetty.http.HttpStatus.REQUEST_TIMEOUT_408
import org.eclipse.jetty.http.HttpStatus.TOO_MANY_REQUESTS_429
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.core5.http.ContentType
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.core5.util.Timeout


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val maxRetries = 10

    private val callTimeoutMs = requestAverageProcessingTime.toMillis() * 2L
    private val connectTimeoutMs = 5_000L
    private val readTimeoutMs = callTimeoutMs

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

    private val semaphore = Semaphore(parallelRequests, true)
    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val cbWaitDuration = Duration.ofSeconds(5)

    private val circuitBreaker: CircuitBreaker = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(100)
            .minimumNumberOfCalls(20)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(80f)
            .slowCallDurationThreshold(Duration.ofMillis(callTimeoutMs))
            .waitDurationInOpenState(cbWaitDuration)
            .permittedNumberOfCallsInHalfOpenState(10)
            .recordExceptions(Exception::class.java)
            .build()
    ).circuitBreaker("payment-$accountName")

    private val scheduledExecutor = ScheduledThreadPoolExecutor(
        10,
        NamedThreadFactory("payment-scheduled-$accountName")
    )

    private val esExecutor: ExecutorService = Executors.newFixedThreadPool(
        8,
        NamedThreadFactory("payment-es-updates-${properties.accountName}")
    )

    private fun submitEsUpdate(block: () -> Unit) {
        esExecutor.submit {
            try {
                block()
            } catch (e: Exception) {
                logger.error("[${properties.accountName}] ES update failed", e)
            }
        }
    }

    private val extTimer = Timer
        .builder("external.payment.duration")
        .description("End-to-end payment duration")
        .tag("accountName", properties.accountName)
        .publishPercentiles(0.5, 0.85, 0.95, 0.99)
        .register(meterRegistry)

    private fun retriesCounter(reason: String) = Counter
        .builder("payment.retries")
        .description("Payment retries counter")
        .tag("accountName", properties.accountName)
        .tag("reason", reason)
        .register(meterRegistry)

    private val activeRequests = AtomicInteger(0)

    init {
        client.start()
        Gauge.builder("payment.active_requests") { activeRequests.get().toDouble() }
            .description("Number of active payment requests")
            .tag("accountName", properties.accountName)
            .register(meterRegistry)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        submitEsUpdate {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        executePaymentWithRetriesAsync(paymentId, amount, transactionId, deadline)
            .whenComplete { result, throwable ->
                submitEsUpdate {
                    try {
                        if (throwable != null) {
                            logger.error("[${properties.accountName}] Payment failed for payment $paymentId", throwable)
                            paymentESService.update(paymentId) {
                                it.logProcessing(
                                    success = false,
                                    processedAt = now(),
                                    transactionId = transactionId,
                                    reason = throwable.message ?: "Exception"
                                )
                            }
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(
                                    success = result.success,
                                    processedAt = now(),
                                    transactionId = transactionId,
                                    reason = result.reason
                                )
                            }
                        }
                    } catch (e: Exception) {
                        logger.error("[${properties.accountName}] Error updating payment state for $paymentId", e)
                    }
                }
            }
    }

    private data class PaymentResult(val success: Boolean, val reason: String?)

    private fun executePaymentWithRetriesAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): CompletableFuture<PaymentResult> {
        return executePaymentWithRetriesAsyncInternal(paymentId, amount, transactionId, deadline, 0, 100L)
    }

    private fun executePaymentWithRetriesAsyncInternal(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        retryCount: Int,
        backoffMs: Long
    ): CompletableFuture<PaymentResult> {
        if (retryCount > maxRetries) {
            return CompletableFuture.completedFuture(
                PaymentResult(false, "Max retries ($maxRetries) exceeded")
            )
        }

        if (now() + backoffMs > deadline) {
            return CompletableFuture.completedFuture(
                PaymentResult(false, "Deadline budget exceeded")
            )
        }


        if (!limiter.tick()) {
            val waitTime = (1000.0 / rateLimitPerSec).toLong().coerceAtMost(100L)
            if (now() + waitTime >= deadline) {
                return CompletableFuture.completedFuture(
                    PaymentResult(false, "Rate limit exceeded, no time budget for retry")
                )
            }

            val future = CompletableFuture<PaymentResult>()
            scheduleRetry(
                delayMs = waitTime,
                paymentId = paymentId,
                amount = amount,
                transactionId = transactionId,
                deadline = deadline,
                retryCount = retryCount,
                backoffMs = backoffMs,
                future = future
            )
            return future
        }


        if (!semaphore.tryAcquire()) {
            val retryDelay = 50L
            if (now() + retryDelay >= deadline) {
                return CompletableFuture.completedFuture(
                    PaymentResult(false, "Parallel request limit exceeded, no time budget for retry")
                )
            }

            val future = CompletableFuture<PaymentResult>()
            scheduleRetry(
                delayMs = retryDelay,
                paymentId = paymentId,
                amount = amount,
                transactionId = transactionId,
                deadline = deadline,
                retryCount = retryCount,
                backoffMs = backoffMs,
                future = future
            )
            return future
        }

        if (!circuitBreaker.tryAcquirePermission()) {
            semaphore.release()
            logger.warn("[$accountName] Circuit breaker OPEN for payment $paymentId, will retry after wait")
            val cbWaitMs = cbWaitDuration.toMillis()
            if (now() + cbWaitMs >= deadline) {
                return CompletableFuture.completedFuture(
                    PaymentResult(false, "Circuit breaker open, deadline exceeded")
                )
            }
            val future = CompletableFuture<PaymentResult>()
            scheduleRetry(cbWaitMs, paymentId, amount, transactionId, deadline, retryCount, backoffMs, future)
            return future
        }

        activeRequests.incrementAndGet()
        val future = CompletableFuture<PaymentResult>()

        val url =
            "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"


        val request = SimpleRequestBuilder.post(url)
            .setBody("", ContentType.TEXT_PLAIN)
            .build()

        val startAll = now()

        client.execute(request, object : FutureCallback<SimpleHttpResponse> {
            override fun completed(response: SimpleHttpResponse) {
                val callDuration = now() - startAll
                releaseSlot()
                recordDuration(startAll)

                try {
                    val status = response.code

                    if (status == TOO_MANY_REQUESTS_429
                        || status == REQUEST_TIMEOUT_408
                        || status >= INTERNAL_SERVER_ERROR_500
                    ) {
                        circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, RuntimeException("HTTP $status"))

                        var newBackoffMs = backoffMs

                        if (status == TOO_MANY_REQUESTS_429) {
                            val raHeader = response.getHeader("Retry-After")
                            val ra = raHeader?.value

                            if (!ra.isNullOrBlank()) {
                                ra.toLongOrNull()?.let {
                                    newBackoffMs = (it * 1000).coerceAtLeast(1000L)
                                }
                            }
                        }

                        if (now() + newBackoffMs < deadline) {
                            val reason = when (status) {
                                TOO_MANY_REQUESTS_429 -> "http_429"
                                REQUEST_TIMEOUT_408 -> "http_408"
                                else -> "http_5xx"
                            }
                            retriesCounter(reason).increment()

                            logger.warn(
                                "[$accountName] HTTP $status, retryAfter=${
                                    response.getFirstHeader("Retry-After")?.value
                                }, willBackoffMs=$newBackoffMs, retryCount=$retryCount for txId=$transactionId"
                            )

                            val nextBackoffMs = (newBackoffMs * 1.5).toLong().coerceAtMost(5000L)

                            scheduleRetry(
                                delayMs = newBackoffMs,
                                paymentId = paymentId,
                                amount = amount,
                                transactionId = transactionId,
                                deadline = deadline,
                                retryCount = retryCount + 1,
                                backoffMs = nextBackoffMs,
                                future = future
                            )
                        } else {
                            future.complete(
                                PaymentResult(false, "HTTP $status without time budget for retry")
                            )
                        }
                        return
                    }

                    val raw = response.bodyText
                    val body = try {
                        mapper.readValue(raw, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                    "payment: $paymentId, result code: $status, reason: $raw"
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    circuitBreaker.onSuccess(callDuration, TimeUnit.MILLISECONDS)

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                "succeeded: ${body.result}, message: ${body.message}"
                    )

                    if (body.result) {
                        future.complete(PaymentResult(true, body.message))
                        return
                    }

                    val isTemporary =
                        body.message?.contains("Temporary", ignoreCase = true) == true
                    if (isTemporary && now() + backoffMs < deadline) {
                        retriesCounter("temporary").increment()
                        val nextBackoffMs = (backoffMs * 1.5).toLong().coerceAtMost(5000L)

                        scheduleRetry(
                            delayMs = backoffMs,
                            paymentId = paymentId,
                            amount = amount,
                            transactionId = transactionId,
                            deadline = deadline,
                            retryCount = retryCount + 1,
                            backoffMs = nextBackoffMs,
                            future = future
                        )
                    } else {
                        future.complete(PaymentResult(false, body.message))
                    }
                } catch (e: Exception) {
                    circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, e)
                    logger.error("[$accountName] Error processing response for txId=$transactionId", e)
                    future.completeExceptionally(e)
                }
            }

            override fun failed(ex: Exception) {
                val callDuration = now() - startAll
                releaseSlot()
                recordDuration(startAll)

                circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, ex)
                logger.warn("[$accountName] Retry: $retryCount: request failed for txId=$transactionId", ex)

                if (retryCount >= maxRetries || now() + backoffMs >= deadline) {
                    future.complete(PaymentResult(false, "Request timeout: ${ex.message}"))
                    return
                }

                retriesCounter("timeout").increment()
                val nextBackoffMs = (backoffMs * 1.5).toLong().coerceAtMost(5000L)

                scheduleRetry(
                    delayMs = backoffMs,
                    paymentId = paymentId,
                    amount = amount,
                    transactionId = transactionId,
                    deadline = deadline,
                    retryCount = retryCount + 1,
                    backoffMs = nextBackoffMs,
                    future = future
                )
            }

            override fun cancelled() {
                val callDuration = now() - startAll
                releaseSlot()
                recordDuration(startAll)
                circuitBreaker.onSuccess(callDuration, TimeUnit.MILLISECONDS)
                future.complete(PaymentResult(false, "Request cancelled"))
            }
        })

        return future
    }

    private fun scheduleRetry(
        delayMs: Long,
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        retryCount: Int,
        backoffMs: Long,
        future: CompletableFuture<PaymentResult>
    ) {
        scheduledExecutor.schedule({
            executePaymentWithRetriesAsyncInternal(
                paymentId,
                amount,
                transactionId,
                deadline,
                retryCount,
                backoffMs
            ).whenComplete { result, throwable ->
                if (throwable != null) {
                    future.completeExceptionally(throwable)
                } else {
                    future.complete(result)
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    private fun releaseSlot() {
        semaphore.release()
        activeRequests.decrementAndGet()
    }

    private fun recordDuration(startAll: Long) {
        val duration = now() - startAll
        extTimer.record(duration, TimeUnit.MILLISECONDS)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()