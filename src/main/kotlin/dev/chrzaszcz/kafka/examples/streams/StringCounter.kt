package dev.chrzaszcz.kafka.examples.streams

import org.apache.kafka.streams.processor.AbstractProcessor

class StringCounter: AbstractProcessor<String, String>() {
    override fun process(key: String?, value: String?) {
        context().forward(key, value?.count())
        context().commit()
    }
}