package dev.chrzaszcz.kafka.examples.simple.consumer

import dev.chrzaszcz.kafka.examples.simple.TOPIC
import mu.KLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class Consumer : Runnable {

    private val shouldConsume = AtomicBoolean(true)

    override fun run() {
        val kafkaConsumer = createKafkaConsumer()
        kafkaConsumer.subscribe(listOf(TOPIC))
        kafkaConsumer.use { fetchContinuously(kafkaConsumer) }
    }

    private fun fetchContinuously(kafkaConsumer: KafkaConsumer<String, String>) {
        while (shouldConsume.get()) {

            try {
                val messages = kafkaConsumer.poll(Duration.ofMillis(1000))

                messages.forEach {
                    logger.info { "Consumed $it" }
                }
            } catch (e: Exception) {
                logger.error { "Failed to consume" }
            }
        }
        logger.info { "Exiting consumer loop" }
    }

    fun stop() {
        shouldConsume.set(false)
    }

    private fun createKafkaConsumer(): KafkaConsumer<String, String> {
        val properties = Properties()

        properties["bootstrap.servers"] = "localhost:9092"
        properties["group.id"] = "kafka-example"
        properties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        properties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

        return KafkaConsumer(properties)
    }

    companion object : KLogging()
}