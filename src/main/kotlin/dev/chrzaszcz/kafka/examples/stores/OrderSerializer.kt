package dev.chrzaszcz.kafka.examples.stores

import org.apache.kafka.common.errors.SerializationException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer


class OrderSerializer(private val objectMapper: ObjectMapper) : Serializer<Order> {
    override fun serialize(topic: String, data: Order?): ByteArray? {
        return data?.let { serialize(it) }
    }

    private fun serialize(data: Order): ByteArray {
        try {
            return objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw SerializationException("Could not serialize Order", e)
        }
    }
}