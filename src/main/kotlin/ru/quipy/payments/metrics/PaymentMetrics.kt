package ru.quipy.payments.metrics

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(private val registry: MeterRegistry) {
    fun incArrivals() =
        registry.counter("payments_submitted_total").increment()
}