package dev.chrzaszcz.kafka.examples.streams

import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*

private val logger = KotlinLogging.logger {}

fun main() {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "count-application"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

    createTopic(props)

    val builder = StreamsBuilder()
    val wordsStream = builder.stream<String, String>("WordsTopic")

    wordsStream
        .mapValues { count -> count.count() }
        .to("CountsTopic", Produced.with(Serdes.String(), Serdes.Integer()))

    val topology = builder.build()

    val streams = KafkaStreams(topology, props)
    streams.start()
}

private fun createTopic(props: Properties) {
    logger.info { "Creating topic" }
    val adminClient = AdminClient.create(props)
    adminClient.createTopics(listOf(NewTopic("WordsTopic", 1, 1)))
}
