package dev.chrzaszcz.kafka.examples.simple

import dev.chrzaszcz.kafka.examples.simple.consumer.Consumer
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main() {
    val consumer = startConsuming()

    Runtime.getRuntime().addShutdownHook(object : Thread() {
        override fun run() {
            logger.info { "Shutting down ..." }
            consumer.stop()
        }
    })
}

private fun startConsuming(): Consumer {
    val consumer = Consumer()
    Thread(consumer).start()
    return consumer
}
