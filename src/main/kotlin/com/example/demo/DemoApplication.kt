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
import java.time.Instant

@SpringBootApplication
class DemoApplication(
    private val paymentProducer: PaymentProducer,
    private val userProducer: UserProducer,
    private val merchantProducer: MerchantProducer
) {

    @Bean
    fun runner() = ApplicationRunner {
        val payment = Payment.newBuilder()
            .setId("1")
            .setCreatedAt(Instant.now())
            .setUpdatedAt(Instant.now())
            .setStatus("PENDING")
            .setUserId("1")
            .setMerchantId("1")
            .setAmount(100)
            .build()

        val user = User.newBuilder()
            .setId("1")
            .setCreatedAt(Instant.now())
            .setUpdatedAt(Instant.now())
            .setName("John Doe")
            .setEmail("john.doe@gmail.com")
            .build()

        val merchant = Merchant.newBuilder()
            .setId("1")
            .setCreatedAt(Instant.now())
            .setUpdatedAt(Instant.now())
            .setName("Amazon")
            .setCategory("E-commerce")
            .build()

        paymentProducer.send(payment)
        userProducer.send(user)
        merchantProducer.send(merchant)
    }


}

fun main(args: Array<String>) {

    runApplication<DemoApplication>(*args)
}
