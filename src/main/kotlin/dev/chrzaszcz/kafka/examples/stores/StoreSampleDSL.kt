package dev.chrzaszcz.kafka.examples.stores

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import java.util.*

private val logger = KotlinLogging.logger {}

fun main() {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "store-sample-dsl-application"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0

    createTopic(props)

    val objectMapper = ObjectMapper().registerModule(KotlinModule())

    val orderSerializer = OrderSerializer(objectMapper)
    val orderDeserializer = OrderDeserializer(objectMapper)

    val orderSerdes = Serdes.serdeFrom(orderSerializer, orderDeserializer)

    val builder = StreamsBuilder()
    val messagesStream = builder.stream<String, Order>("Orders", Consumed.with(Serdes.String(), orderSerdes))

    messagesStream
        .filter { _, value -> value.meal == "pizza" }
        .groupBy { _, value -> value.customerId }
        .count(Materialized
            .`as`<String, Long, KeyValueStore<Bytes, ByteArray>> ("TotalPizzaOrdersStore")
            .withCachingDisabled())
        .toStream()
        .to("TotalPizzaOrders", Produced.with(Serdes.String(), Serdes.Long()))

    val topology = builder.build()

    logger.info { topology.describe() }

    val streams = KafkaStreams(topology, props)
    streams.start()
}

private fun createTopic(props: Properties) {
    logger.info { "Creating topic" }
    val adminClient = AdminClient.create(props)
    adminClient.createTopics(listOf(NewTopic("Orders", 1, 1)))
}

data class Order(
    val customerId: String,
    val meal: String
)