package com.example.demo

import com.example.demo.avro.Payment
import com.example.demo.producer.PaymentProducer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

@EnableScheduling
@SpringBootApplication
class DemoApplication(
    private val paymentProducer: PaymentProducer,
) {
    private val users = listOf("u1", "u2", "u3")

    @Scheduled(fixedRate = 5000)
    fun producePayment() {
        paymentProducer.send(
            Payment
                .newBuilder()
                .setId(UUID.randomUUID().toString())
                .setDatetime(Instant.now())
                .setUserId(users.random())
                .setAmount(Random.nextInt(1, 1000).toLong())
                .build(),
        )
    }
}

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
