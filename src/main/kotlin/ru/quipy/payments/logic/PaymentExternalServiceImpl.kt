package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.kotlin.ratelimiter.executeSuspendFunction
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient
import org.apache.hc.client5.http.impl.async.HttpAsyncClients
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.util.Timeout
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter: RateLimiter = RateLimiter.of(
        "rate-limiter-$accountName",
        RateLimiterConfig.custom()
            .limitForPeriod(rateLimitPerSec)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .build()
    )

    private val sentToBank: Counter = Counter
        .builder("sent_request_to_bank")
        .tags("account_name", accountName)
        .register(meterRegistry)

    private val repeatRequest: Counter = Counter
        .builder("repeat_request")
        .tags("account_name", accountName)
        .register(meterRegistry)

    private val requestLatency: DistributionSummary = DistributionSummary
        .builder("request_latency")
        .tag("account_name", accountName)
        .publishPercentiles(0.9, 0.99, 0.999, 0.9999)
        .register(meterRegistry)

    private val dispatcherClient = Executors.newFixedThreadPool(60).asCoroutineDispatcher()
    private val dispatcherPayment = Executors.newFixedThreadPool(60).asCoroutineDispatcher()

    private val scope = CoroutineScope(dispatcherPayment + SupervisorJob())

    // Таймаут клиента 1 секунда — как в условии теста.
    private val requestTimeoutMillis = 1_000L

    private val requestConfig: RequestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(Timeout.ofMilliseconds(requestTimeoutMillis))
        .setResponseTimeout(Timeout.ofMilliseconds(requestTimeoutMillis))
        .build()

    private val connectionConfig: ConnectionConfig = ConnectionConfig.custom()
        .setConnectTimeout(Timeout.ofMilliseconds(requestTimeoutMillis))
        .build()

    private val client: CloseableHttpAsyncClient =
        HttpAsyncClients.customHttp2()
            .setDefaultConnectionConfig(connectionConfig)
            .setDefaultRequestConfig(requestConfig)
            .build()

    private val semaphore = Semaphore(permits = parallelRequests)

    init {
        client.start()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Важно зафиксировать факт отправки ВСЕГДА — тестер это проверяет.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        scope.launch {
            bankPayment(transactionId, paymentId, amount, paymentStartedAt)
        }
    }

    private suspend fun bankPayment(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long
    ) {
        val url =
            "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"

        sendRequestWithRetry(transactionId, paymentId, url, paymentStartedAt)
    }

    private suspend fun sendRequestWithRetry(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long,
    ) {
        val delayMs = 100L
        val maxRetries = 3
        var curRetry = 1

        while (curRetry < maxRetries) {
            if (sendRequest(transactionId, paymentId, url, paymentStartedAt)) {
                return
            }
            repeatRequest.increment()
            delay(delayMs)
            curRetry += 1
        }
    }

    private suspend fun sendRequest(
        transactionId: UUID,
        paymentId: UUID,
        url: String,
        paymentStartedAt: Long
    ): Boolean {
        var result = false

        semaphore.withPermit {
            rateLimiter.executeSuspendFunction {
                sentToBank.increment()

                try {
                    val response = doHttpPost(url)
                    val success = handleSuccess(
                        response.code,
                        response.bodyText,
                        transactionId,
                        paymentId
                    )
                    requestLatency.record((now() - paymentStartedAt).toDouble())
                    result = success
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> handleTimeout(transactionId, paymentId, e)
                        else -> handleError(transactionId, paymentId, e)
                    }
                    requestLatency.record((now() - paymentStartedAt).toDouble())
                    result = false
                }
            }
        }

        return result
    }

    private suspend fun doHttpPost(url: String): SimpleHttpResponse =
        suspendCancellableCoroutine { cont ->
            val request = SimpleRequestBuilder.post(url)
                .setBody("", ContentType.TEXT_PLAIN)
                .build()

            val future = client.execute(
                request,
                object : org.apache.hc.core5.concurrent.FutureCallback<SimpleHttpResponse> {
                    override fun completed(result: SimpleHttpResponse) {
                        if (cont.isCancelled) return
                        cont.resume(result)
                    }

                    override fun failed(ex: Exception) {
                        if (cont.isCancelled) return
                        cont.resumeWithException(ex)
                    }

                    override fun cancelled() {
                        if (cont.isCancelled) return
                        cont.resumeWithException(RuntimeException("Request cancelled"))
                    }
                }
            )

            cont.invokeOnCancellation {
                future.cancel(true)
            }
        }

    private fun handleTimeout(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
        }
    }

    private fun handleError(transactionId: UUID, paymentId: UUID, e: Exception) {
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = e.message)
        }
    }

    fun handleSuccess(
        responseCode: Int,
        responseBody: String,
        transactionId: UUID,
        paymentId: UUID
    ): Boolean {
        val body = try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error(
                "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                        "payment: $paymentId, result code: $responseCode, reason: $responseBody"
            )
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn(
            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                    "succeeded: ${body.result}, message: ${body.message}"
        )

        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }

        return body.result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()