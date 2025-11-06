package ru.quipy.apigateway

import io.prometheus.metrics.core.metrics.Summary
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.metrics.PaymentMetrics
import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.http.HttpHeaders
import ru.quipy.payments.logic.TooManyRequestsException

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var metrics: PaymentMetrics

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    companion object {
        private val payOrderLatency: Summary = Summary.builder()
            .name("http_request_latent")
            .help("Latent time before submit (client->shop).")
            .quantile(0.5, 0.01)
            .quantile(0.85, 0.005)
            .quantile(0.95, 0.005)
            .quantile(0.99, 0.001)
            .register()
    }

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")


        metrics.incArrivals()
        return try {
            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
        } catch (e: TooManyRequestsException) {
            val headers = HttpHeaders()
            headers.add(HttpHeaders.RETRY_AFTER, e.retryAfterMillis.toString())
            ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).headers(headers).build()
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}