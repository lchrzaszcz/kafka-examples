package dev.chrzaszcz.kafka.examples.streams

import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import java.util.*

private val logger = KotlinLogging.logger {}

fun main() {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "count-application"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

    createTopic(props)

    val topology = Topology()

    topology.addSource("mySource", "WordsTopic")

    topology.addProcessor("myCharacterCounter", ProcessorSupplier { StringCounter() }, "mySource")
    topology.addProcessor("myOddChecker", ProcessorSupplier { OddChecker() }, "mySource")

    topology.addSink(
        "myCharacterCountSink",
        "CountsTopic",
        Serdes.String().serializer(),
        Serdes.Integer().serializer(),
        "myCharacterCounter"
    )

    topology.addSink(
        "myOddSink",
        "OddTopic",
        Serdes.String().serializer(),
        Serdes.Integer().serializer(),
        "myOddChecker"
    )

    logger.info { topology.describe() }

    val streams = KafkaStreams(topology, props)
    streams.start()
}

private fun createTopic(props: Properties) {
    logger.info { "Creating topic" }
    val adminClient = AdminClient.create(props)
    adminClient.createTopics(listOf(NewTopic("WordsTopic", 1, 1)))
}

