package dev.chrzaszcz.kafka.examples

import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

private val logger = KotlinLogging.logger {}

fun main() {
    try {
        produce()
    } catch (e: Throwable) {
        logger.error(e) {}
    }
}

private fun produce() {
    val properties = Properties()

    properties["bootstrap.servers"] = "localhost:9092"
    properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    properties["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

    val kafkaConsumer = KafkaProducer<String, String>(properties)

    val messages = listOf(1, 2, 3, 4)

    messages
        .map { it.toString() }
        .map { ProducerRecord<String, String>(TOPIC, it) }
        .map {
            logger.info { "Sending $it" }
            kafkaConsumer.send(it)
        }
        .forEach { it.get() }
}