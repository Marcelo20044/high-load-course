package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.util.EntityUtils
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
    private val maxRetries = 5

    private val callTimeoutMs = requestAverageProcessingTime.toMillis() * 2L
    private val connectTimeoutMs = 5_000L
    private val readTimeoutMs = callTimeoutMs
    private val writeTimeoutMs = 5_000L

    private val requestConfig = RequestConfig.custom()
        .setConnectTimeout(connectTimeoutMs.toInt())
        .setSocketTimeout(readTimeoutMs.toInt())
        .setConnectionRequestTimeout(connectTimeoutMs.toInt())
        .build()

    private val client: CloseableHttpAsyncClient = HttpAsyncClients.custom()
        .setMaxConnTotal(parallelRequests)
        .setMaxConnPerRoute(parallelRequests)
        .setDefaultRequestConfig(requestConfig)
        .build()

    private val semaphore = Semaphore(parallelRequests, true)
    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val scheduledExecutor = ScheduledThreadPoolExecutor(
        10,
        NamedThreadFactory("payment-scheduled-$accountName")
    )

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

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        executePaymentWithRetriesAsync(paymentId, amount, transactionId, deadline)
            .whenComplete { result, throwable ->
                try {
                    if (throwable != null) {
                        logger.error("[$accountName] Payment failed for payment $paymentId", throwable)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = throwable.message ?: "Exception")
                        }
                    } else {
                        paymentESService.update(paymentId) {
                            it.logProcessing(result.success, now(), transactionId, reason = result.reason)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error updating payment state for $paymentId", e)
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
            return CompletableFuture.completedFuture(PaymentResult(false, "Max retries ($maxRetries) exceeded"))
        }

        if (now() + backoffMs > deadline) {
            return CompletableFuture.completedFuture(PaymentResult(false, "Deadline budget exceeded"))
        }

        // Rate limiting
        if (!limiter.tick()) {
            val waitTime = (1000.0 / rateLimitPerSec).toLong().coerceAtMost(100L)
            if (now() + waitTime >= deadline) {
                return CompletableFuture.completedFuture(
                    PaymentResult(
                        false,
                        "Rate limit exceeded, no time budget for retry"
                    )
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

        // Parallelism limiting
        if (!semaphore.tryAcquire()) {
            val retryDelay = 50L
            if (now() + retryDelay >= deadline) {
                return CompletableFuture.completedFuture(
                    PaymentResult(
                        false,
                        "Parallel request limit exceeded, no time budget for retry"
                    )
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

        activeRequests.incrementAndGet()
        val future = CompletableFuture<PaymentResult>()

        val url =
            "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        val httpPost = HttpPost(url)
        httpPost.entity = StringEntity("", "UTF-8")

        val startAll = now()

        client.execute(httpPost, object : FutureCallback<HttpResponse> {
            override fun completed(response: HttpResponse) {
                releaseSlot()
                recordDuration(startAll)

                try {
                    val status = response.statusLine.statusCode

                    if (status == TOO_MANY_REQUESTS_429 || status == REQUEST_TIMEOUT_408 || status >= INTERNAL_SERVER_ERROR_500) {
                        var newBackoffMs = backoffMs

                        if (status == TOO_MANY_REQUESTS_429) {
                            val ra = response.getFirstHeader("Retry-After")?.value
                            if (!ra.isNullOrBlank()) {
                                ra.toLongOrNull()?.let { newBackoffMs = (it * 1000).coerceAtLeast(1000L) }
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

                            val nextBackoffMs =
                                (newBackoffMs * 1.5).toLong().coerceAtMost(5000L)

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
                                PaymentResult(
                                    false,
                                    "HTTP $status without time budget for retry"
                                )
                            )
                        }
                        return
                    }

                    val raw = EntityUtils.toString(response.entity)
                    val body = try {
                        mapper.readValue(raw, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusLine.statusCode}, reason: $raw"
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    if (body.result) {
                        future.complete(PaymentResult(true, body.message))
                        return
                    }

                    val isTemporary =
                        body.message?.contains("Temporary", ignoreCase = true) == true
                    if (isTemporary && now() + backoffMs < deadline) {
                        retriesCounter("temporary").increment()
                        val nextBackoffMs =
                            (backoffMs * 1.5).toLong().coerceAtMost(5000L)

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
                    logger.error("[$accountName] Error processing response for txId=$transactionId", e)
                    future.completeExceptionally(e)
                }
            }

            override fun failed(ex: Exception) {
                releaseSlot()
                recordDuration(startAll)

                logger.error("[$accountName] Retry: $retryCount: request failed for txId=$transactionId", ex)

                if (retryCount >= maxRetries || now() + backoffMs >= deadline) {
                    future.complete(PaymentResult(false, "Request timeout: ${ex.message}"))
                    return
                }

                retriesCounter("timeout").increment()
                val nextBackoffMs =
                    (backoffMs * 1.5).toLong().coerceAtMost(5000L)

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
                releaseSlot()
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