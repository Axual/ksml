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

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.KSMLRunnerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

    static KSMLRunnerWrapper ksmlRunner;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        // Create topics first
        createTopics();

        // Prepare test environment with Binary-specific files
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/binary";
        String[] binaryFiles = {"ksml-runner.yaml", "binary-producer.yaml", "binary-processor.yaml"};

        Path configPath = KSMLRunnerTestUtil.prepareTestEnvironment(
                tempDir,
                resourcePath,
                binaryFiles,
                kafka.getBootstrapServers()
        );

        log.info("Using KSMLRunner directly with config: {}", configPath);

        // Start KSML using KSMLRunner main method
        ksmlRunner = new KSMLRunnerWrapper(configPath);
        ksmlRunner.start();

        // Wait for KSML to be ready
        waitForKSMLReady();
    }

    @Test
    void testKSMLBinaryProcessing() throws Exception {
        // Wait for first binary data to be generated and processed
        log.info("Waiting for KSML to generate and process binary data...");
        waitForBinaryDataGeneration();

        // Verify KSML is still running
        assertThat(ksmlRunner.isRunning()).isTrue().as("KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Check ksml_sensordata_binary topic (producer output - binary data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-binary");
        Map<String, byte[]> originalMessages = new LinkedHashMap<>(); // Use LinkedHashMap to preserve insertion order
        List<String> originalMessageOrder = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_binary"));
            ConsumerRecords<String, byte[]> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).isNotEmpty().as("Should have generated binary data in ksml_sensordata_binary topic");
            log.info("Found {} binary messages", records.count());

            // Validate binary messages and store for comparison
            records.forEach(record -> {
                log.info("Binary message: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertThat(record.key()).startsWith("msg").as("Message key should start with 'msg'");

                // Validate binary structure - should be 7 bytes with 'KSML' pattern
                byte[] binaryValue = record.value();
                assertThat(binaryValue).hasSize(7).as("Binary message should have 7 bytes");

                // Check that bytes 2-5 are ASCII 'KSML' (K=75, S=83, M=77, L=76)
                // Compare the KSML portion as a byte array
                byte[] ksmlPortion = Arrays.copyOfRange(binaryValue, 2, 6);
                byte[] expectedKsml = {75, 83, 77, 76}; // ASCII 'KSML'
                assertThat(ksmlPortion).isEqualTo(expectedKsml).as("Bytes 2-5 should be ASCII 'KSML'");

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

            assertThat(records).isNotEmpty().as("Should have processed binary data in ksml_sensordata_binary_processed topic");
            log.info("Found {} processed binary messages", records.count());

            // Validate processed binary messages against originals
            records.forEach(record -> {
                // Track processing order
                processedMessageOrder.add(record.key());
                log.info("Processed binary: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertThat(record.key()).startsWith("msg").as("Message key should start with 'msg'");

                // Validate binary structure contains processed data
                byte[] processedValue = record.value();
                assertThat(processedValue).hasSize(7).as("Processed binary message should have 7 bytes");

                // Check that bytes 2-5 are still ASCII 'KSML' (unchanged by processor)
                byte[] processedKsmlPortion = Arrays.copyOfRange(processedValue, 2, 6);
                byte[] expectedKsml = {75, 83, 77, 76}; // ASCII 'KSML'
                assertThat(processedKsmlPortion).isEqualTo(expectedKsml).as("Bytes 2-5 should still be ASCII 'KSML'");

                // Verify transformation: first byte should be incremented by 1 (with wrap-around)
                byte[] originalValue = originalMessages.get(record.key());
                assertThat(originalValue).isNotNull().as("Should have original message for key: " + record.key());

                int originalFirstByte = originalValue[0] & 0xFF;
                int processedFirstByte = processedValue[0] & 0xFF;
                int expectedProcessedFirstByte = (originalFirstByte + 1) % 256;

                assertThat(processedFirstByte).isEqualTo(expectedProcessedFirstByte)
                    .as("First byte should be incremented: original=%d, expected=%d, actual=%d",
                        originalFirstByte, expectedProcessedFirstByte, processedFirstByte);

                // Verify bytes 1 and 6 remain unchanged (random bytes should be preserved)
                assertThat(processedValue[1]).isEqualTo(originalValue[1]).as("Second byte should remain unchanged");
                assertThat(processedValue[6]).isEqualTo(originalValue[6]).as("Seventh byte should remain unchanged");
            });

            // Verify that processed messages maintain the same order as original messages
            assertThat(processedMessageOrder).isEqualTo(originalMessageOrder)
                .as("Processed messages should maintain the same order as original messages");
            log.info("✅ Message ordering verified: {} messages processed in correct order", processedMessageOrder.size());
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("Binary data format processing test completed successfully!");
        log.info("KSML generated binary data using binary-producer.yaml");
        log.info("KSML processed binary data and incremented first byte using binary-processor.yaml");
        log.info("Binary transformation and processing working correctly");
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSMLRunner to be ready...");

        // Use the built-in wait method from the wrapper
        ksmlRunner.waitForReady(15000); // 15 seconds timeout

        log.info("KSMLRunner is ready");
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

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("ksml_sensordata_binary", 1, (short) 1),
                    new NewTopic("ksml_sensordata_binary_processed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: ksml_sensordata_binary, ksml_sensordata_binary_processed");
        }
    }

    @AfterAll
    static void cleanup() {
        if (ksmlRunner != null) {
            log.info("Stopping KSMLRunner...");
            ksmlRunner.stop();
        }
    }
}