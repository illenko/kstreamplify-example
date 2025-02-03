package com.example.demo.streams

import com.example.demo.avro.EnrichedPayment
import com.example.demo.avro.Payment
import com.example.demo.avro.User
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class Streams : KafkaStreamsStarter() {
    override fun topology(builder: StreamsBuilder) {
        val payments =
            builder
                .stream("payments", Consumed.with<String, Payment>(Serdes.String(), SerdesUtils.getValueSerdes()))
                .selectKey { _, it -> it.userId }

        val users =
            builder.table("users", Consumed.with<String, User>(Serdes.String(), SerdesUtils.getValueSerdes()))

        payments
            .join(
                users,
                { p, u ->
                    EnrichedPayment
                        .newBuilder()
                        .setPayment(p)
                        .setUser(u)
                        .build()
                },
                Joined.with(Serdes.String(), SerdesUtils.getValueSerdes(), SerdesUtils.getValueSerdes()),
            ).selectKey { _, it -> it.payment.id }
            .to("enriched-payments", Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()))

        val paymentsGroupedByUser =
            payments.groupByKey(
                Grouped.with(Serdes.String(), SerdesUtils.getValueSerdes()),
            )

        paymentsGroupedByUser.aggregate(
            { 0L },
            { _, p, totalAmount -> totalAmount + p.amount },
            Materialized
                .`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("user-spending")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()),
        )

        paymentsGroupedByUser
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(5)))
            .count(Materialized.`as`("user-payments"))
    }

    override fun dlqTopic(): String = "dlq-topic"
}
