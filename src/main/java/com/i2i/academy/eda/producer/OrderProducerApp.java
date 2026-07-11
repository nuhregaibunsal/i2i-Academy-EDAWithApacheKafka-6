package com.i2i.academy.eda.producer;

import com.i2i.academy.eda.config.KafkaConfig;
import com.i2i.academy.eda.model.Order;
import com.i2i.academy.eda.serialization.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Producer application.
 *
 * <p>Publishes a batch of custom {@link Order} objects to the {@code orders} topic.
 * Each order is serialized to JSON by {@link JsonSerializer} and keyed by its order id
 * so that all events for the same order land on the same partition.</p>
 *
 * <p>Run with:
 * {@code mvn exec:java -Dexec.mainClass="com.i2i.academy.eda.producer.OrderProducerApp"}</p>
 */
public class OrderProducerApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        List<Order> orders = sampleOrders();

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            for (Order order : orders) {
                ProducerRecord<String, Order> record =
                        new ProducerRecord<>(KafkaConfig.ORDERS_TOPIC, order.getOrderId(), order);

                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to publish order " + order.getOrderId() + ": " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "Published order %s -> topic=%s partition=%d offset=%d%n",
                                order.getOrderId(), metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
            System.out.println("All orders published.");
        }
    }

    private static List<Order> sampleOrders() {
        return List.of(
                new Order(UUID.randomUUID().toString(), "Ada Lovelace", "Mechanical Keyboard", 1, new BigDecimal("129.90"), Instant.now()),
                new Order(UUID.randomUUID().toString(), "Alan Turing", "USB-C Hub", 2, new BigDecimal("39.50"), Instant.now()),
                new Order(UUID.randomUUID().toString(), "Grace Hopper", "27\" Monitor", 1, new BigDecimal("249.00"), Instant.now()),
                new Order(UUID.randomUUID().toString(), "Linus Torvalds", "Laptop Stand", 3, new BigDecimal("24.99"), Instant.now())
        );
    }
}
