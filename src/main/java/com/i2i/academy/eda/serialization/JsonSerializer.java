package com.i2i.academy.eda.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic Kafka {@link Serializer} that turns any custom Java object into JSON bytes
 * using Jackson. Used by the Producer to publish {@code Order} objects.
 *
 * @param <T> the type of object being serialized
 */
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing object to JSON for topic " + topic, e);
        }
    }
}
