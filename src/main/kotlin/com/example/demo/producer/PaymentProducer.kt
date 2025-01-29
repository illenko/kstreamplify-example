package com.example.demo.producer

import com.example.demo.avro.Payment
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class PaymentProducer(
    private val kafkaTemplate: KafkaTemplate<String, Payment>,
) {
    fun send(payment: Payment) = kafkaTemplate.send("payments", payment.id, payment)
}
