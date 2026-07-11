package com.i2i.academy.eda.config;

/**
 * Shared Kafka configuration constants used by both the Producer and Consumer apps.
 */
public final class KafkaConfig {

    /** Broker address exposed by the local Docker Compose setup. */
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /** Topic on which custom {@code Order} objects are published. */
    public static final String ORDERS_TOPIC = "orders";

    /** Consumer group id for the Order consumer. */
    public static final String CONSUMER_GROUP_ID = "order-consumer-group";

    private KafkaConfig() {
        // Utility class - no instances.
    }
}
