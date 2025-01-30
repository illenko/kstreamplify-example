package com.example.demo.streams

import com.example.demo.avro.Payment
import com.example.demo.avro.UserPayments
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.stereotype.Component

@Component
class Streams : KafkaStreamsStarter() {
    override fun topology(builder: StreamsBuilder) {
        val payments =
            builder
                .stream("payments", Consumed.with<String, Payment>(Serdes.String(), SerdesUtils.getValueSerdes()))
                .selectKey { _, it -> it.userId }

        payments.groupByKey(
            Grouped.with(Serdes.String(), SerdesUtils.getValueSerdes())
        ).aggregate(
            {
                UserPayments
                    .newBuilder()
                    .setCount(0L)
                    .setAmount(0L)
                    .build()
            },
            { _, p, t ->
                UserPayments
                    .newBuilder()
                    .setCount(t.count + 1)
                    .setAmount(t.amount + p.amount)
                    .build()
            },
            Materialized
                .`as`<String, UserPayments, KeyValueStore<Bytes, ByteArray>>("user-payments")
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesUtils.getValueSerdes()),
        )
    }

    override fun dlqTopic(): String = "dlq-topic"
}
