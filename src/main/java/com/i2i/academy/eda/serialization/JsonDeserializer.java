package com.i2i.academy.eda.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Generic Kafka {@link Deserializer} that reconstructs custom Java objects from JSON
 * bytes using Jackson. Used by the Consumer to read {@code Order} objects back.
 *
 * <p>The target type is provided via the {@code value.deserializer.type} consumer
 * property, or through the constructor when configured programmatically.</p>
 *
 * @param <T> the type of object being deserialized
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    public static final String VALUE_TYPE_CONFIG = "value.deserializer.type";

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Class<T> targetType;

    public JsonDeserializer() {
        // Type resolved later via configure(...).
    }

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object type = configs.get(VALUE_TYPE_CONFIG);
        if (type instanceof Class<?> clazz) {
            this.targetType = (Class<T>) clazz;
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (targetType == null) {
            throw new SerializationException("Target type not configured for JsonDeserializer");
        }
        try {
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON from topic " + topic, e);
        }
    }
}
