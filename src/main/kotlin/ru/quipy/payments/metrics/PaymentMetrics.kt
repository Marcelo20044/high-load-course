package ru.quipy.payments.metrics

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(private val registry: MeterRegistry) {
    fun incArrivals() =
        registry.counter("payments_submitted_total").increment()
    fun recordLimiterWaitMs(account: String, waitedMs: Double) {
        DistributionSummary.builder("payments_limiter_wait_ms")
            .description("Сколько ждали до сети на лимитере/в очереди, мс")
            .tags("account", account)
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)
            .record(waitedMs)
    }

    fun recordBackoffMs(account: String, reason: String, backoffMs: Double) {
        DistributionSummary.builder("payments_backoff_ms")
            .description("Пауза перед ретраем, мс")
            .tags("account", account, "reason", reason)
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)
            .record(backoffMs)
    }

    fun recordDeadlineRemainingMs(account: String, remainMs: Double) {
        DistributionSummary.builder("payments_deadline_remaining_ms")
            .description("Остаток бюджета при уходе в сеть, мс")
            .tags("account", account)
            .publishPercentiles(0.5, 0.9, 0.99)
            .register(registry)
            .record(remainMs)
    }

    fun incRetrySuppressedByDeadline(account: String) {
        registry.counter("payments_retry_suppressed_total", "account", account, "reason", "deadline").increment()
    }

    fun recordRequestLatency(account: String, statusCode: String, latencyMs: Double) {
        DistributionSummary.builder("request_latency")
            .description("Request latency")
            .tags("account", account, "status_code", statusCode)
            .publishPercentiles(0.5, 0.8, 0.99)
            .register(registry)
            .record(latencyMs)
    }

    fun incRetry(account: String, reason: String? = null) {
        if (reason != null) {
            registry.counter("payment_retries_total", "account", account, "reason", reason).increment()
        } else {
            registry.counter("payment_retries_total", "account", account).increment()
        }
    }
}