package com.example.demo.streams

import com.example.demo.avro.Payment
import com.example.demo.avro.PaymentWithUser
import com.example.demo.avro.User
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced
import org.springframework.stereotype.Component

@Component
class Streams : KafkaStreamsStarter() {
    override fun topology(builder: StreamsBuilder) {

        val payments =
            builder.stream("payments", Consumed.with<String, Payment>(Serdes.String(), SerdesUtils.getValueSerdes()))
                .selectKey { _, it ->
                    it.userId
                }

        val users =
            builder.table("users", Consumed.with<String, User>(Serdes.String(), SerdesUtils.getValueSerdes()))

        val paymentsWithUsers = payments.join(
            users,
            { payment, user -> PaymentWithUser.newBuilder().setPayment(payment).setUser(user).build() },
            Joined.with(Serdes.String(), SerdesUtils.getValueSerdes(), SerdesUtils.getValueSerdes())
        )

        paymentsWithUsers.to("payments-with-users", Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()))
    }


    override fun dlqTopic(): String = "dlq-topic"
}