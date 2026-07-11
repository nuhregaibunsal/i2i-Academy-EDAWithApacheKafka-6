package com.i2i.academy.eda.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Custom domain object exchanged over Kafka.
 *
 * <p>The Producer publishes {@code Order} instances to the topic and the Consumer
 * reads them back, demonstrating how custom Java objects travel through a Kafka
 * topic once they are (de)serialized as JSON.</p>
 */
public class Order {

    private String orderId;
    private String customer;
    private String product;
    private int quantity;
    private BigDecimal price;
    private Instant createdAt;

    public Order() {
        // Required by Jackson for deserialization.
    }

    public Order(String orderId, String customer, String product, int quantity, BigDecimal price, Instant createdAt) {
        this.orderId = orderId;
        this.customer = customer;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.createdAt = createdAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public BigDecimal totalAmount() {
        if (price == null) {
            return BigDecimal.ZERO;
        }
        return price.multiply(BigDecimal.valueOf(quantity));
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customer='" + customer + '\'' +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", total=" + totalAmount() +
                ", createdAt=" + createdAt +
                '}';
    }
}
