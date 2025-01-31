package com.example.demo.streams

import com.example.demo.avro.Payment
import com.example.demo.avro.PaymentWithUser
import com.example.demo.avro.User
import com.michelin.kstreamplify.KafkaStreamsStarterTest
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StreamsTest : KafkaStreamsStarterTest() {
    private lateinit var usersTopic: TestInputTopic<String, User>
    private lateinit var paymentsTopic: TestInputTopic<String, Payment>
    private lateinit var paymentWithUserTopic: TestOutputTopic<String, PaymentWithUser>

    override fun getKafkaStreamsStarter(): KafkaStreamsStarter = Streams()

    @BeforeEach
    fun before() {
        usersTopic =
            testDriver.createInputTopic(
                "users",
                Serdes.String().serializer(),
                SerdesUtils.getValueSerdes<User>().serializer(),
            )
        paymentsTopic =
            testDriver.createInputTopic(
                "payments",
                Serdes.String().serializer(),
                SerdesUtils.getValueSerdes<Payment>().serializer(),
            )
        paymentWithUserTopic =
            testDriver.createOutputTopic(
                "payments-with-users",
                Serdes.String().deserializer(),
                SerdesUtils.getValueSerdes<PaymentWithUser>().deserializer(),
            )
    }

    @Test
    fun `should join payments with users`() {
        val user =
            User
                .newBuilder()
                .setId("u1")
                .setCreatedAt(Instant.parse("2025-01-03T00:00:00Z"))
                .setUpdatedAt(Instant.now())
                .setName("John Doe")
                .setEmail("john.doe@gmail.com")
                .build()

        val payment1 =
            Payment
                .newBuilder()
                .setId("p1")
                .setDatetime(Instant.now())
                .setUserId("u1")
                .setAmount(100)
                .build()
        val payment2 =
            Payment
                .newBuilder()
                .setId("p2")
                .setDatetime(Instant.now())
                .setUserId("u1")
                .setAmount(200)
                .build()
        val payment3 =
            Payment
                .newBuilder()
                .setId("p3")
                .setDatetime(Instant.now())
                .setUserId("u2")
                .setAmount(300)
                .build()

        usersTopic.pipeInput(user.id, user)
        paymentsTopic.pipeInput(payment1.id, payment1)
        paymentsTopic.pipeInput(payment2.id, payment2)
        paymentsTopic.pipeInput(payment3.id, payment3)

        val paymentWithUser1 = paymentWithUserTopic.readKeyValuesToList()
        val paymentWithUser2 = paymentWithUserTopic.readKeyValuesToList()

        assertEquals(2, paymentWithUser1.size)

        assertEquals("p1", paymentWithUser1[0].key)
        assertEquals(payment1, paymentWithUser1[0].value.payment)
        assertEquals(user, paymentWithUser1[0].value.user)

        assertEquals("p2", paymentWithUser1[1].key)
        assertEquals(payment2, paymentWithUser1[1].value.payment)
        assertEquals(user, paymentWithUser1[1].value.user)

        assertTrue { paymentWithUser2.isEmpty() }
    }
}
