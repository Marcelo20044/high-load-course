package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500
import org.eclipse.jetty.http.HttpStatus.REQUEST_TIMEOUT_408
import org.eclipse.jetty.http.HttpStatus.TOO_MANY_REQUESTS_429
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val maxRetries = 5

    private val observedP85Ms = 1000L
    private val requestTimeout = (observedP85Ms * 1.2).toLong()

    private val client = OkHttpClient.Builder().build()
    private var semaphore = Semaphore(parallelRequests, true)
    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

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

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        semaphore.acquire()

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val result = executePaymentWithRetries(paymentId, amount, transactionId, deadline)

            paymentESService.update(paymentId) {
                it.logProcessing(result.success, now(), transactionId, reason = result.reason)
            }
        } finally {
            semaphore.release()
        }
    }

    private data class PaymentResult(val success: Boolean, val reason: String?)

    private fun executePaymentWithRetries(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): PaymentResult {
        var retryCount = 0
        var backoffMs = 100L

        while (retryCount <= maxRetries) {
            if (now() + backoffMs > deadline) {
                return PaymentResult(false, "Deadline budget exceeded")
            }

            limiter.tickBlocking(Duration.ofMillis(backoffMs))

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val startAll = now()
            try {
                val call = client.newCall(request)
                call.timeout().timeout(requestTimeout, TimeUnit.MILLISECONDS)

                call.execute().use { response ->
                    val status = response.code
                    extTimer.record(now() - startAll, TimeUnit.MILLISECONDS)

                    if (status == TOO_MANY_REQUESTS_429 || status == REQUEST_TIMEOUT_408 || status >= INTERNAL_SERVER_ERROR_500) {

                        if (status == TOO_MANY_REQUESTS_429) {
                            val ra = response.header("Retry-After")
                            if (!ra.isNullOrBlank()) {
                                ra.toLongOrNull()?.let { backoffMs = (it * 1000).coerceAtLeast(1000L) }
                            }
                        }

                        if (now() + backoffMs < deadline) {
                            val reason = when (status) {
                                TOO_MANY_REQUESTS_429 -> "http_429"
                                REQUEST_TIMEOUT_408 -> "http_408"
                                else -> "http_5xx"
                            }
                            retriesCounter(reason).increment()

                            logger.warn("[$accountName] HTTP $status, retryAfter=${response.header("Retry-After")}, willBackoffMs=$backoffMs, retryCount=$retryCount for txId=$transactionId")
                            Thread.sleep(backoffMs)
                            retryCount++
                            continue
                        }
                        return PaymentResult(false, "HTTP $status without time budget for retry")
                    }

                    val raw = response.body?.string() ?: ""
                    val body = try {
                        mapper.readValue(raw, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        return PaymentResult(true, body.message)
                    }

                    val isTemporary = body.message?.contains("Temporary", ignoreCase = true) == true
                    if (isTemporary) {
                        retriesCounter("temporary").increment()
                        if (now() + backoffMs < deadline) {
                            Thread.sleep(backoffMs)
                            retryCount++
                            continue
                        }
                    }

                    return PaymentResult(false, body.message)
                }
            } catch (e: Exception) {
                extTimer.record(now() - startAll, TimeUnit.MILLISECONDS)
                logger.error("[$accountName] Retry: ${retryCount}: request failed", e)

                if (retryCount >= maxRetries || now() + backoffMs >= deadline) {
                    return PaymentResult(false, "Request timeout.")
                }
                retriesCounter("timeout").increment()
                Thread.sleep(backoffMs)
                retryCount++
            }
        }

        return PaymentResult(false, "Max retries ($maxRetries) exceeded")
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()