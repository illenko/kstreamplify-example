package com.example.demo

import com.example.demo.avro.Merchant
import com.example.demo.avro.Payment
import com.example.demo.avro.User
import com.example.demo.producer.MerchantProducer
import com.example.demo.producer.PaymentProducer
import com.example.demo.producer.UserProducer
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import java.time.Instant
import kotlin.math.abs
import kotlin.random.Random

@EnableScheduling
@SpringBootApplication
class DemoApplication(
    private val paymentProducer: PaymentProducer,
    private val userProducer: UserProducer,
    private val merchantProducer: MerchantProducer
) {

    private val merchants = listOf(
        Merchant.newBuilder()
            .setId("m1")
            .setCreatedAt(Instant.parse("2025-01-01T00:00:00Z"))
            .setUpdatedAt(Instant.now())
            .setName("Amazon")
            .setCategory("E-commerce")
            .build(),
        Merchant.newBuilder()
            .setId("m2")
            .setCreatedAt(Instant.parse("2025-01-02T00:00:00Z"))
            .setUpdatedAt(Instant.now())
            .setName("eBay")
            .setCategory("E-commerce")
            .build()
    )

    private val users = listOf(
        User.newBuilder()
            .setId("u1")
            .setCreatedAt(Instant.parse("2025-01-03T00:00:00Z"))
            .setUpdatedAt(Instant.now())
            .setName("John Doe")
            .setEmail("john.doe@gmail.com")
            .build(),
        User.newBuilder()
            .setId("u2")
            .setCreatedAt(Instant.parse("2025-01-04T00:00:00Z"))
            .setUpdatedAt(Instant.now())
            .setName("Jane Smith")
            .setEmail("jane.smith@gmail.com")
            .build(),
        User.newBuilder()
            .setId("u3")
            .setCreatedAt(Instant.parse("2025-01-05T00:00:00Z"))
            .setUpdatedAt(Instant.now())
            .setName("Alice Johnson")
            .setEmail("alice.johnson@gmail.com")
            .build()
    )

    val statuses = listOf("PENDING", "APPROVED", "REJECTED")

    @Bean
    fun runner() = ApplicationRunner {
        merchants.forEach { merchantProducer.send(it) }
        users.forEach { userProducer.send(it) }
    }

    @Scheduled(fixedRate = 5000)
    fun producePayment() {
        val user = users.random()
        val merchant = merchants.random()
        val payment = Payment.newBuilder()
            .setId("p${abs(Random.nextInt())}")
            .setCreatedAt(Instant.now())
            .setUpdatedAt(Instant.now())
            .setStatus(statuses.random())
            .setUserId(user.id)
            .setMerchantId(merchant.id)
            .setAmount(Random.nextInt(1, 1000).toLong())
            .build()

        paymentProducer.send(payment)
    }
}

fun main(args: Array<String>) {

    runApplication<DemoApplication>(*args)
}
