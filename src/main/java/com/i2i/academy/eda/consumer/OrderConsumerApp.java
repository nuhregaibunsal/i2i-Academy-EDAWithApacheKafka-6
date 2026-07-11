package com.i2i.academy.eda.consumer;

import com.i2i.academy.eda.config.KafkaConfig;
import com.i2i.academy.eda.model.Order;
import com.i2i.academy.eda.serialization.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Consumer application.
 *
 * <p>Subscribes to the {@code orders} topic, deserializes each record back into a
 * custom {@link Order} object via {@link JsonDeserializer} and prints it to the console.
 * The loop runs until the process is stopped (Ctrl+C).</p>
 *
 * <p>Run with:
 * {@code mvn exec:java -Dexec.mainClass="com.i2i.academy.eda.consumer.OrderConsumerApp"}</p>
 */
public class OrderConsumerApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_TYPE_CONFIG, Order.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaConfig.ORDERS_TOPIC));
            System.out.println("Listening for orders on topic '" + KafkaConfig.ORDERS_TOPIC + "'... (Ctrl+C to stop)");

            // Ensure a clean shutdown so the consumer leaves the group gracefully.
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            try {
                while (true) {
                    ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, Order> record : records) {
                        Order order = record.value();
                        System.out.printf(
                                "Received [partition=%d offset=%d key=%s] %s%n",
                                record.partition(), record.offset(), record.key(), order);
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // Expected on shutdown - ignore.
            }
        }
        System.out.println("Consumer stopped.");
    }
}
