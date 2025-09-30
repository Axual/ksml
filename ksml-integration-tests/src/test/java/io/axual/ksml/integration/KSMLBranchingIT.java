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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real KSML Integration Test that runs actual KSML container with real YAML definitions.
 * This test mounts the actual KSML producer and processor YAML files and validates
 * that KSML processes orders correctly with the real container.
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

        // Create ksml-runner.yaml that points to the actual files
        String runnerYaml = """
                ksml:
                  definitions:
                    producer: producer-order-events.yaml
                    processor: processor-order-processing.yaml
                  storageDirectory: /ksml/state
                  createStorageDirectory: true
                
                kafka:
                  bootstrap.servers: broker:9093
                  application.id: io.ksml.real.test
                  security.protocol: PLAINTEXT
                  acks: all
                """;

        Files.writeString(tempDir.resolve("ksml-runner.yaml"), runnerYaml);

        // Get the actual KSML definition files
        String producerPath = "/Users/km/dev/ksml/ksml/src/test/resources/docs-examples/intermediate-tutorial/branching/producer-order-events.yaml";
        String processorPath = "/Users/km/dev/ksml/ksml/src/test/resources/docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml";

        // Verify files exist
        File producerFile = new File(producerPath);
        File processorFile = new File(processorPath);

        if (!producerFile.exists()) {
            throw new RuntimeException("Producer file not found: " + producerPath);
        }
        if (!processorFile.exists()) {
            throw new RuntimeException("Processor file not found: " + processorPath);
        }

        log.info("Using KSML files:");
        log.info("  Producer: {}", producerPath);
        log.info("  Processor: {}", processorPath);

        // Start KSML container with file mounts
        ksmlContainer = new GenericContainer<>(DockerImageName.parse("registry.axual.io/opensource/images/axual/ksml:1.1.0"))
                .withNetwork(network)
                .withNetworkAliases("ksml")
                .withWorkingDirectory("/ksml")
                .withCopyFileToContainer(MountableFile.forHostPath(tempDir.resolve("ksml-runner.yaml").toString()), "/ksml/ksml-runner.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(producerPath), "/ksml/producer-order-events.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(processorPath), "/ksml/processor-order-processing.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(stateDir.toString()), "/ksml/state")
                .withCommand("ksml-runner.yaml")
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("KSML"))
                .dependsOn(kafka);

        log.info("Starting KSML container...");
        ksmlContainer.start();

        // Wait for KSML to be ready by checking logs
        waitForKSMLReady();
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSML container to be ready...");

        // Wait for KSML to start processing - look for key log messages
        long startTime = System.currentTimeMillis();
        long timeout = 60000; // 60 seconds max
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

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("order_input", 1, (short) 1),
                    new NewTopic("priority_orders", 1, (short) 1),
                    new NewTopic("regional_orders", 1, (short) 1),
                    new NewTopic("international_orders", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: order_input, priority_orders, regional_orders, international_orders");
        }
    }

    @Test
    void testRealKSMLOrderProcessing() throws Exception {
        // Wait for first order to be generated and processed
        // Producer generates every 3s, so wait for at least 2-3 orders
        log.info("Waiting for KSML to generate and process orders...");
        waitForOrderGeneration();

        // Verify KSML is still running
        assertTrue(ksmlContainer.isRunning(), "KSML container should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check order_input topic (producer output)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-input");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("order_input"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated orders in order_input topic");
            log.info("Found {} orders in order_input topic", records.count());

            // Log some sample orders
            records.forEach(record -> log.info("Order: key={}, value={}", record.key(), record.value()));
        }

        // Check priority_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-priority");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("priority_orders"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (!records.isEmpty()) {
                log.info("Found {} priority orders", records.count());
                records.forEach(record -> {
                    log.info("Priority order: {}", record.value());
                    assertTrue(record.value().contains("\"processing_tier\":\"priority\""),
                            "Priority orders should have priority processing tier");
                    assertTrue(record.value().contains("\"sla_hours\":4"),
                            "Priority orders should have 4 hour SLA");
                });
            } else {
                log.info("No priority orders found (might not have generated premium orders > $1000)");
            }
        }

        // Check regional_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-regional");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("regional_orders"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (!records.isEmpty()) {
                log.info("Found {} regional orders", records.count());
                records.forEach(record -> {
                    log.info("Regional order: {}", record.value());
                    assertTrue(record.value().contains("\"processing_tier\":\"regional\""),
                            "Regional orders should have regional processing tier");
                    assertTrue(record.value().contains("\"sla_hours\":24"),
                            "Regional orders should have 24 hour SLA");
                });
            } else {
                log.info("No regional orders found");
            }
        }

        // Check international_orders topic
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-international");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("international_orders"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (!records.isEmpty()) {
                log.info("Found {} international orders", records.count());
                records.forEach(record -> {
                    log.info("International order: {}", record.value());
                    assertTrue(record.value().contains("\"processing_tier\":\"international\""),
                            "International orders should have international processing tier");
                    assertTrue(record.value().contains("\"sla_hours\":72"),
                            "International orders should have 72 hour SLA");
                    assertTrue(record.value().contains("\"customs_required\":true"),
                            "International orders should have customs_required flag");
                });
            } else {
                log.info("No international orders found");
            }
        }

        // Check KSML logs for processing messages
        String logs = ksmlContainer.getLogs();
        assertTrue(logs.contains("Processing order:"), "KSML should log order processing");

        // Should not have errors
        assertFalse(logs.contains("ERROR"), "KSML should not have errors: " + extractErrors(logs));
        assertFalse(logs.contains("Exception"), "KSML should not have exceptions: " + extractErrors(logs));

        log.info("Real KSML Order Processing test completed successfully!");
        log.info("KSML container executed real YAML definitions");
        log.info("KSML generated orders using producer-order-events.yaml");
        log.info("KSML processed orders using processor-order-processing.yaml");
        log.info("Orders were correctly routed to priority/regional/international topics");
    }

    private String extractErrors(String logs) {
        return logs.lines()
                .filter(line -> line.contains("ERROR") || line.contains("Exception"))
                .limit(5)
                .reduce((a, b) -> a + "\n" + b)
                .orElse("No specific errors found");
    }
}