package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val counter = AtomicLong(0)

    override suspend fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        val enabled = paymentAccounts.filter { it.isEnabled() }
        if (enabled.isEmpty()) {
            logger.error("No enabled payment accounts for payment $paymentId")
            return
        }
        val account = enabled[(counter.getAndIncrement() % enabled.size).toInt()]

        // performPaymentAsync стал suspend — не блокирует поток во время HTTP-вызова.
        // Deadline передаём внутрь: там можно использовать withTimeout(deadline - now())
        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}