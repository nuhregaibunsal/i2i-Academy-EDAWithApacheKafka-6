# EDA with Apache Kafka

Event-Driven Architecture (EDA) demo built with **plain Java** and **Apache Kafka**.
A **Producer** application publishes custom Java objects (`Order`) to a Kafka topic, and a
**Consumer** application reads those objects back and prints them to the console.

> i2i Academy homework project. This repository contains only source code and
> configuration — the assignment document is intentionally excluded.

## Project scope

- Run an Apache Kafka broker locally via Docker Compose.
- A Java Producer that serializes custom `Order` objects to JSON and publishes them
  to the `orders` topic.
- A Java Consumer that subscribes to the topic, deserializes the messages back into
  `Order` objects, and prints them.

## Tech stack

- Java 17
- Apache Kafka client (`kafka-clients`)
- Jackson (JSON serialization of custom objects)
- Docker Compose (Kafka broker + Kafka UI)
- Maven

## Status

🚧 Work in progress — see commit history for incremental development.
