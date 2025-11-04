package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500
import org.eclipse.jetty.http.HttpStatus.REQUEST_TIMEOUT_408
import org.eclipse.jetty.http.HttpStatus.TOO_MANY_REQUESTS_429
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
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

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(1))
        .writeTimeout(Duration.ofSeconds(2))
        .readTimeout(Duration.ofSeconds(15))
        .callTimeout(Duration.ofSeconds(20))
        .build()

    private var semaphore = Semaphore(parallelRequests, true)
    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val remainingForAcquire = (deadline - now()).coerceAtLeast(0L)
        val acquired = if (remainingForAcquire > 0) {
            semaphore.tryAcquire(remainingForAcquire, TimeUnit.MILLISECONDS)
        } else false

        if (!acquired) {
            logger.warn("[$accountName] Could not acquire semaphore within deadline for payment $paymentId")
            return
        }

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

            try {
                val remain = (deadline - now()).coerceAtLeast(1L)
                val call = client.newCall(request)
                call.timeout().timeout(remain, TimeUnit.MILLISECONDS)
                call.execute().use { response ->
                    val status = response.code

                    if (status == TOO_MANY_REQUESTS_429 || status == REQUEST_TIMEOUT_408 || status >= INTERNAL_SERVER_ERROR_500) {

                        if (status == TOO_MANY_REQUESTS_429) {
                            val ra = response.header("Retry-After")
                            if (!ra.isNullOrBlank()) {
                                ra.toLongOrNull()?.let { backoffMs = (it * 1000).coerceAtLeast(1000L) }
                            }
                        }

                        if (now() + backoffMs < deadline) {
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
                        if (now() + backoffMs < deadline) {
                            Thread.sleep(backoffMs)
                            retryCount++
                            continue
                        }
                    }

                    return PaymentResult(false, body.message)
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)

                if (retryCount >= maxRetries) {
                    return PaymentResult(false, "Request timeout - max retries ($maxRetries) exceeded")
                }

                if (now() + backoffMs < deadline) {
                    Thread.sleep(backoffMs)
                    retryCount++
                    continue
                }

                return PaymentResult(false, "Request timeout.")
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                return PaymentResult(false, e.message)
            }
        }

        return PaymentResult(false, "Max retries ($maxRetries) exceeded")
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()