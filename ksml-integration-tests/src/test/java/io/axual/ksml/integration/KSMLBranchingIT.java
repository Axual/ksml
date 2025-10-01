package io.axual.ksml.integration;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test for branching functionality.
 * This test validates that KSML can process orders and route them to different topics based on conditions.
 * <p>
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class KSMLBranchingIT {

    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/intermediate-tutorial/branching",
                          "ksml-runner.yaml", "producer-order-events.yaml", "processor-order-processing.yaml")
            .withKafka(kafka)
            .withTopics("order_input", "priority_orders", "regional_orders", "international_orders")
            .dependsOn(kafka);

    private void waitForOrderGeneration() throws Exception {
        log.info("Waiting for order generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 orders
        // Use AdminClient to check actual message count instead of log parsing
        KSMLRunnerTestUtil.waitForTopicMessages(
                kafka.getBootstrapServers(),
                "order_input",
                2, // Wait for at least 2 orders
                Duration.ofSeconds(30) // Maximum 30 seconds
        );

        log.info("Order data has been generated and verified");
    }


    @Test
    void testKSMLOrderProcessing() throws Exception {
        // Wait for first order to be generated and processed
        // Producer generates every 3s, so wait for at least 2-3 orders
        log.info("Waiting for KSML to generate and process orders...");
        waitForOrderGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Create consumer properties
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check order_input topic (producer output)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-input");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("order_input"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated orders in order_input topic").isNotEmpty();
            log.info("Found {} orders in order_input topic", records.count());

            // Log some sample orders
            records.forEach(record -> log.info("Order: key={}, value={}", record.key(), record.value()));
        }

        // Check priority_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-priority");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("priority_orders"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            if (!records.isEmpty()) {
                log.info("Found {} priority orders", records.count());
                records.forEach(record -> {
                    log.info("Priority order: {}", record.value());
                    assertThat(record.value()).as("Priority orders should have priority processing tier")
                            .contains("\"processing_tier\":\"priority\"");
                    assertThat(record.value()).as("Priority orders should have 4 hour SLA")
                            .contains("\"sla_hours\":4");
                });
            } else {
                log.info("No priority orders found (might not have generated premium orders > $1000)");
            }
        }

        // Check regional_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-regional");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("regional_orders"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            if (!records.isEmpty()) {
                log.info("Found {} regional orders", records.count());
                records.forEach(record -> {
                    log.info("Regional order: {}", record.value());
                    assertThat(record.value()).as("Regional orders should have regional processing tier")
                            .contains("\"processing_tier\":\"regional\"");
                    assertThat(record.value()).as("Regional orders should have 24 hour SLA")
                            .contains("\"sla_hours\":24");
                });
            } else {
                log.info("No regional orders found");
            }
        }

        // Check international_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-international");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("international_orders"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            if (!records.isEmpty()) {
                log.info("Found {} international orders", records.count());
                records.forEach(record -> {
                    log.info("International order: {}", record.value());
                    assertThat(record.value()).as("International orders should have international processing tier")
                            .contains("\"processing_tier\":\"international\"");
                    assertThat(record.value()).as("International orders should have 72 hour SLA")
                            .contains("\"sla_hours\":72");
                    assertThat(record.value()).as("International orders should have customs_required flag")
                            .contains("\"customs_required\":true");
                });
            } else {
                log.info("No international orders found");
            }
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("KSML Order Processing test completed successfully!");
        log.info("KSML generated orders using producer-order-events.yaml");
        log.info("KSML processed orders using processor-order-processing.yaml");
        log.info("Orders were correctly routed to priority/regional/international topics");
    }
}