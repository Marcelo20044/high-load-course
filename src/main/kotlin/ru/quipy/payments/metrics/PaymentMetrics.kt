package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadPoolExecutor

@Component
class PaymentMetrics(private val registry: MeterRegistry) {
    fun incArrivals() =
        registry.counter("payments_submitted_total").increment()

    fun registerExecutorGauges(
        executor: ThreadPoolExecutor,
        tags: List<Tag> = emptyList()
    ) {
        Gauge.builder("payments_queue_size", executor.queue) { it.size.toDouble() }
            .tags(tags).register(registry)

        registry.counter("payments_enqueued_total", tags)
    }

    fun incEnqueued(tags: List<Tag> = emptyList()) =
        registry.counter("payments_enqueued_total", tags).increment()
}