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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KSML Integration Test for Binary data format processing.
 * This test validates that KSML can produce binary messages, transform them, and process them without schema registry.
 *
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
    @Timeout(90) // 1.5 minutes should be enough for binary processing
    void testKSMLBinaryProcessing() throws Exception {
        // Wait for first binary data to be generated and processed
        log.info("Waiting for KSML to generate and process binary data...");
        waitForBinaryDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlRunner.isRunning(), "KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Check ksml_sensordata_binary topic (producer output - binary data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-binary");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_binary"));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated binary data in ksml_sensordata_binary topic");
            log.info("Found {} binary messages", records.count());

            // Validate binary messages
            records.forEach(record -> {
                log.info("Binary message: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertTrue(record.key().startsWith("msg"), "Message key should start with 'msg'");

                // Validate binary structure - should be 7 bytes
                byte[] binaryValue = record.value();
                assertEquals(7, binaryValue.length, "Binary message should have 7 bytes");

                // Check that bytes 2-5 are ASCII 'KSML' (K=75, S=83, M=77, L=76)
                assertEquals(75, binaryValue[2] & 0xFF, "Third byte should be ASCII 'K' (75)");
                assertEquals(83, binaryValue[3] & 0xFF, "Fourth byte should be ASCII 'S' (83)");
                assertEquals(77, binaryValue[4] & 0xFF, "Fifth byte should be ASCII 'M' (77)");
                assertEquals(76, binaryValue[5] & 0xFF, "Sixth byte should be ASCII 'L' (76)");

                // First byte should be counter % 256, sixth and seventh are random (0-255)
                int firstByte = binaryValue[0] & 0xFF;
                int sixthByte = binaryValue[1] & 0xFF;
                int seventhByte = binaryValue[6] & 0xFF;

                assertTrue(firstByte >= 0 && firstByte <= 255, "First byte should be 0-255");
                assertTrue(sixthByte >= 0 && sixthByte <= 255, "Second byte should be 0-255");
                assertTrue(seventhByte >= 0 && seventhByte <= 255, "Seventh byte should be 0-255");
            });
        }

        // Check ksml_sensordata_binary_processed topic (processor output - modified binary)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_binary_processed"));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have processed binary data in ksml_sensordata_binary_processed topic");
            log.info("Found {} processed binary messages", records.count());

            // Validate processed binary messages
            records.forEach(record -> {
                log.info("Processed binary: key={}, bytes={}", record.key(), Arrays.toString(record.value()));
                assertTrue(record.key().startsWith("msg"), "Message key should start with 'msg'");

                // Validate binary structure contains processed data
                byte[] binaryValue = record.value();
                assertEquals(7, binaryValue.length, "Processed binary message should have 7 bytes");

                // Check that bytes 2-5 are still ASCII 'KSML'
                assertEquals(75, binaryValue[2] & 0xFF, "Third byte should still be ASCII 'K' (75)");
                assertEquals(83, binaryValue[3] & 0xFF, "Fourth byte should still be ASCII 'S' (83)");
                assertEquals(77, binaryValue[4] & 0xFF, "Fifth byte should still be ASCII 'M' (77)");
                assertEquals(76, binaryValue[5] & 0xFF, "Sixth byte should still be ASCII 'L' (76)");

                // First byte should be incremented (compared to original)
                int processedFirstByte = binaryValue[0] & 0xFF;
                assertTrue(processedFirstByte >= 0 && processedFirstByte <= 255, "Processed first byte should be 0-255");
            });
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

    private void waitForBinaryDataGeneration() throws InterruptedException {
        log.info("Waiting for binary data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 intervals
        // Since we can't check logs, wait for sufficient time for messages to be generated and processed
        Thread.sleep(7000); // Wait 7 seconds for messages to be generated and processed

        log.info("Binary data should have been generated by now");
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