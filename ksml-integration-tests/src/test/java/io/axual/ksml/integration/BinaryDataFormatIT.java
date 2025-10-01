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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test for Binary data format processing.
 * This test validates that KSML can produce binary messages, transform them, and process them without schema registry.
 * <p>
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class BinaryDataFormatIT {

    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/binary",
                          "ksml-runner.yaml", "binary-producer.yaml", "binary-processor.yaml")
            .withKafka(kafka)
            .withTopics("ksml_sensordata_binary", "ksml_sensordata_binary_processed")
            .dependsOn(kafka);

    @Test
    void testKSMLBinaryProcessing() throws Exception {
        // Wait for first binary data to be generated and processed
        log.info("Waiting for KSML to generate and process binary data...");
        waitForBinaryDataGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Create consumer properties
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Check ksml_sensordata_binary topic (producer output - binary data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-binary");
        final Map<String, byte[]> originalMessages = new LinkedHashMap<>(); // Use LinkedHashMap to preserve insertion order
        final List<String> originalMessageOrder = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_binary"));
            ConsumerRecords<String, byte[]> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated binary data in ksml_sensordata_binary topic").isNotEmpty();
            log.info("Found {} binary messages", records.count());

            // Validate binary messages and store for comparison
            records.forEach(record -> {
                log.info("Binary message: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertThat(record.key()).as("Message key should start with 'msg'").startsWith("msg");

                // Validate binary structure - should be 7 bytes with 'KSML' pattern
                final byte[] binaryValue = record.value();
                assertThat(binaryValue).as("Binary message should have 7 bytes").hasSize(7);

                // Check that bytes 2-5 are ASCII 'KSML' (K=75, S=83, M=77, L=76)
                // Compare the KSML portion as a byte array
                final byte[] ksmlPortion = Arrays.copyOfRange(binaryValue, 2, 6);
                byte[] expectedKsml = {75, 83, 77, 76}; // ASCII 'KSML'
                assertThat(ksmlPortion).as("Bytes 2-5 should be ASCII 'KSML'").isEqualTo(expectedKsml);

                // Store original message for later comparison (preserving order)
                originalMessages.put(record.key(), Arrays.copyOf(binaryValue, binaryValue.length));
                originalMessageOrder.add(record.key());
            });
        }

        // Check ksml_sensordata_binary_processed topic (processor output - modified binary)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        List<String> processedMessageOrder = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_binary_processed"));
            ConsumerRecords<String, byte[]> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have processed binary data in ksml_sensordata_binary_processed topic").isNotEmpty();
            log.info("Found {} processed binary messages", records.count());

            // Validate processed binary messages against originals
            records.forEach(record -> {
                // Track processing order
                processedMessageOrder.add(record.key());
                log.info("Processed binary: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertThat(record.key()).as("Message key should start with 'msg'").startsWith("msg");

                // Validate binary structure contains processed data
                byte[] processedValue = record.value();
                assertThat(processedValue).as("Processed binary message should have 7 bytes").hasSize(7);

                // Check that bytes 2-5 are still ASCII 'KSML' (unchanged by processor)
                byte[] processedKsmlPortion = Arrays.copyOfRange(processedValue, 2, 6);
                byte[] expectedKsml = {75, 83, 77, 76}; // ASCII 'KSML'
                assertThat(processedKsmlPortion).as("Bytes 2-5 should still be ASCII 'KSML'").isEqualTo(expectedKsml);

                // Verify transformation: first byte should be incremented by 1 (with wrap-around)
                byte[] originalValue = originalMessages.get(record.key());
                assertThat(originalValue).as("Should have original message for key: " + record.key()).isNotNull();

                int originalFirstByte = originalValue[0] & 0xFF;
                int processedFirstByte = processedValue[0] & 0xFF;
                int expectedProcessedFirstByte = (originalFirstByte + 1) % 256;

                assertThat(processedFirstByte)
                    .as("First byte should be incremented: original=%d, expected=%d, actual=%d",
                        originalFirstByte, expectedProcessedFirstByte, processedFirstByte)
                    .isEqualTo(expectedProcessedFirstByte);

                // Verify bytes 1 and 6 remain unchanged (random bytes should be preserved)
                assertThat(processedValue[1]).as("Second byte should remain unchanged").isEqualTo(originalValue[1]);
                assertThat(processedValue[6]).as("Seventh byte should remain unchanged").isEqualTo(originalValue[6]);
            });

            // Verify that processed messages maintain the same order as original messages
            assertThat(processedMessageOrder)
                .as("Processed messages should maintain the same order as original messages")
                .isEqualTo(originalMessageOrder);
            log.info("Message ordering verified: {} messages processed in correct order", processedMessageOrder.size());
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("Binary data format processing test completed successfully!");
        log.info("KSML generated binary data using binary-producer.yaml");
        log.info("KSML processed binary data and incremented first byte using binary-processor.yaml");
        log.info("Binary transformation and processing working correctly");
    }

    private void waitForBinaryDataGeneration() throws Exception {
        log.info("Waiting for binary data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 messages
        // Use AdminClient to check actual message count instead of fixed sleep
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "ksml_sensordata_binary",
            2, // Wait for at least 2 messages
            Duration.ofSeconds(30) // Maximum 30 seconds (much better than fixed 7s)
        );

        log.info("Binary data has been generated and verified");
    }

}