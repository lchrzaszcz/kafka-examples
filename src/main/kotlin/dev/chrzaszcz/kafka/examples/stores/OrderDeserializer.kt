package dev.chrzaszcz.kafka.examples.stores

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer


class OrderDeserializer(private val objectMapper: ObjectMapper) : Deserializer<Order> {
    override fun deserialize(topic: String, bytes: ByteArray?): Order? {
        return bytes?.let { deserialize(it) }
    }

    private fun deserialize(bytes: ByteArray): Order {
        try {
            return objectMapper.readValue(bytes)
        } catch (e: Exception) {
            throw SerializationException("Could not deserialize Order", e)
        }
    }
}