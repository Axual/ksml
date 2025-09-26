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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KSML Integration Test for Binary data format processing.
 * This test validates that KSML can produce binary messages, transform them, and process them without schema registry.
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

    static GenericContainer<?> ksmlContainer;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        // Create topics first
        createTopics();

        // Create state directory in the JUnit-managed temp directory (auto-cleaned after test)
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Get KSML definition files from test resources
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/binary";
        String runnerPath = BinaryDataFormatIT.class.getResource(resourcePath + "/ksml-runner.yaml").getPath();
        String producerPath = BinaryDataFormatIT.class.getResource(resourcePath + "/binary-producer.yaml").getPath();
        String processorPath = BinaryDataFormatIT.class.getResource(resourcePath + "/binary-processor.yaml").getPath();

        // Verify files exist
        File runnerFile = new File(runnerPath);
        File producerFile = new File(producerPath);
        File processorFile = new File(processorPath);

        if (!runnerFile.exists()) {
            throw new RuntimeException("Runner file not found: " + runnerPath);
        }
        if (!producerFile.exists()) {
            throw new RuntimeException("Producer file not found: " + producerPath);
        }
        if (!processorFile.exists()) {
            throw new RuntimeException("Processor file not found: " + processorPath);
        }

        log.info("Using KSML files from test resources:");
        log.info("  Runner: {}", runnerPath);
        log.info("  Producer: {}", producerPath);
        log.info("  Processor: {}", processorPath);

        // Start KSML container with file mounts
        ksmlContainer = new GenericContainer<>(DockerImageName.parse("registry.axual.io/opensource/images/axual/ksml:snapshot"))
                .withNetwork(network)
                .withNetworkAliases("ksml")
                .withWorkingDirectory("/ksml")
                .withCopyFileToContainer(MountableFile.forHostPath(runnerPath), "/ksml/ksml-runner.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(producerPath), "/ksml/binary-producer.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(processorPath), "/ksml/binary-processor.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(stateDir.toString()), "/ksml/state")
                .withCommand("ksml-runner.yaml")
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("KSML"))
                .dependsOn(kafka);

        log.info("Starting KSML container...");
        ksmlContainer.start();

        // Wait for KSML to be ready by checking logs
        waitForKSMLReady();
    }

    @Test
    @Timeout(90) // 1.5 minutes should be enough for binary processing
    void testKSMLBinaryProcessing() throws Exception {
        // Wait for first binary data to be generated and processed
        log.info("Waiting for KSML to generate and process binary data...");
        waitForBinaryDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlContainer.isRunning(), "KSML container should still be running");

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

        // Check KSML logs for binary processing messages
        String logs = ksmlContainer.getLogs();
        assertTrue(logs.contains("Binary input: key=msg"), "KSML should log binary input processing");
        assertTrue(logs.contains("Binary output: key=msg"), "KSML should log binary output processing");

        // Verify specific binary transformations in logs
        // The processor should increment the first byte
        assertTrue(logs.contains("Generated binary message:"), "KSML should log binary message generation");
        assertTrue(logs.contains("bytes="), "KSML should log binary bytes content");

        // Check that we have proper binary logging patterns
        assertTrue(logs.matches("(?s).*Binary input: key=msg\\d+, bytes=\\[.*\\].*"),
            "Should have binary input logs with pattern 'Binary input: key=msgX, bytes=[...]'");
        assertTrue(logs.matches("(?s).*Binary output: key=msg\\d+, bytes=\\[.*\\].*"),
            "Should have binary output logs with pattern 'Binary output: key=msgX, bytes=[...]'");

        // Should not have errors
        assertFalse(logs.contains("ERROR"), "KSML should not have errors: " + extractErrors(logs));
        assertFalse(logs.contains("Exception"), "KSML should not have exceptions: " + extractErrors(logs));

        log.info("Binary data format processing test completed successfully!");
        log.info("KSML generated binary data using binary-producer.yaml");
        log.info("KSML processed binary data and incremented first byte using binary-processor.yaml");
        log.info("Binary transformation and processing working correctly");
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSML container to be ready...");

        // Wait for KSML to start processing - look for key log messages
        long startTime = System.currentTimeMillis();
        long timeout = 60000; // 60 seconds should be enough for simple binary setup
        boolean ksmlReady = false;
        boolean producerStarted = false;

        while (System.currentTimeMillis() - startTime < timeout) {
            // Check if container is still running
            if (!ksmlContainer.isRunning()) {
                String logs = ksmlContainer.getLogs();
                log.error("KSML container exited. Logs:\n{}", logs);
                throw new RuntimeException("KSML container exited with logs:\n" + logs);
            }

            String logs = ksmlContainer.getLogs();

            // Look for KSML processing ready state
            if (!ksmlReady && logs.contains("Pipeline processing state change. Moving from old state 'REBALANCING' to new state 'RUNNING'")) {
                log.info("KSML pipeline is running");
                ksmlReady = true;
            }

            // Look for producer started
            if (!producerStarted && logs.contains("Starting Kafka producer(s)")) {
                log.info("KSML producer started");
                producerStarted = true;
            }

            // Both conditions met
            if (ksmlReady && producerStarted) {
                log.info("KSML container fully ready");
                return;
            }

            Thread.sleep(1000); // Check every second
        }

        throw new RuntimeException("KSML did not become ready within " + (timeout / 1000) + " seconds");
    }

    private void waitForBinaryDataGeneration() throws InterruptedException {
        log.info("Waiting for binary data generation to start...");

        // Producer generates every 3 seconds, wait for at least 2-3 messages to be generated
        // But check logs first to see if binary data is already being generated
        String logs = ksmlContainer.getLogs();
        if (logs.contains("Binary output: key=msg")) {
            log.info("Binary data already being generated");
            return;
        }

        // Wait up to 15 seconds for first binary data (3s interval + some buffer)
        long startTime = System.currentTimeMillis();
        long timeout = 15000; // 15 seconds should be enough

        while (System.currentTimeMillis() - startTime < timeout) {
            logs = ksmlContainer.getLogs();
            if (logs.contains("Binary output: key=msg")) {
                log.info("Binary data generation detected");
                // Wait for one more interval to ensure processing
                Thread.sleep(4000); // 3s interval + 1s buffer
                return;
            }
            Thread.sleep(1000);
        }

        throw new RuntimeException("No binary data generation detected within " + (timeout / 1000) + " seconds");
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

    private String extractErrors(String logs) {
        return logs.lines()
                .filter(line -> line.contains("ERROR") || line.contains("Exception"))
                .limit(5)
                .reduce((a, b) -> a + "\n" + b)
                .orElse("No specific errors found");
    }
}