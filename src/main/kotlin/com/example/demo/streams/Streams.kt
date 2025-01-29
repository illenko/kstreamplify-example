package com.example.demo.streams

import com.example.demo.avro.*
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.stereotype.Component

@Component
class Streams : KafkaStreamsStarter() {
    override fun topology(builder: StreamsBuilder) {

        val payments =
            builder.stream("payments", Consumed.with<String, Payment>(Serdes.String(), SerdesUtils.getValueSerdes()))
                .selectKey { _, it -> it.userId }

        val users =
            builder.table("users", Consumed.with<String, User>(Serdes.String(), SerdesUtils.getValueSerdes()))

        val userPayments = payments.join(
            users,
            { p, u -> PaymentWithUser.newBuilder().setPayment(p).setUser(u).build() },
            Joined.with(Serdes.String(), SerdesUtils.getValueSerdes(), SerdesUtils.getValueSerdes())
        )

        userPayments.groupByKey().aggregate(
            {
                UserPayments.newBuilder()
                    .setCount(0L)
                    .setAmount(0L)
                    .build()
            },
            { _, up, t ->
                UserPayments.newBuilder()
                    .setCount(t.count + 1)
                    .setAmount(t.amount + up.payment.amount)
                    .build()
            },
            Materialized.`as`<String, UserPayments, KeyValueStore<Bytes, ByteArray>>("user-payments")
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesUtils.getValueSerdes())

        )
    }


    override fun dlqTopic(): String = "dlq-topic"
}