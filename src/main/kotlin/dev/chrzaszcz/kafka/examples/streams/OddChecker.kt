package dev.chrzaszcz.kafka.examples.streams

import org.apache.kafka.streams.processor.AbstractProcessor

class OddChecker: AbstractProcessor<String, String>() {
    override fun process(key: String?, value: String?) {
        val count = value?.count()
        context().forward(key, count?.rem(2))
        context().commit()
    }
}