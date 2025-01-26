package com.example.demo.producer

import com.example.demo.avro.Merchant
import com.example.demo.avro.Payment
import com.example.demo.avro.User
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class PaymentProducer(private val kafkaTemplate: KafkaTemplate<String, Payment>) {

    fun send(payment: Payment) =
        kafkaTemplate.send("payments", payment.id, payment)

}

@Component
class UserProducer(private val kafkaTemplate: KafkaTemplate<String, User>) {

    fun send(user: User) =
        kafkaTemplate.send("users", user.id, user)

}

@Component
class MerchantProducer(private val kafkaTemplate: KafkaTemplate<String, Merchant>) {

    fun send(merchant: Merchant) =
        kafkaTemplate.send("merchants", merchant.id, merchant)

}