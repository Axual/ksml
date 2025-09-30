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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.SoftAssertions;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test with JsonSchema and Apicurio Schema Registry.
 * This test validates that KSML can produce JsonSchema messages, transform them, and process them using schema registry.
 * <p>
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class JsonSchemaRegistryIT {

    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>("apicurio/apicurio-registry:3.0.2")
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("QUARKUS_HTTP_PORT", "8081")
            .withEnv("QUARKUS_HTTP_CORS_ORIGINS", "*")
            .withEnv("QUARKUS_PROFILE", "prod")
            .withEnv("APICURIO_STORAGE_KIND", "sql")
            .withEnv("APICURIO_STORAGE_SQL_KIND", "h2")
            .waitingFor(Wait.forHttp("/apis").forPort(8081).withStartupTimeout(Duration.ofMinutes(2)));

    static KSMLRunnerWrapper ksmlRunner;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        // Create topics first
        createTopics();

        // Register JsonSchema manually
        registerJsonSchema();

        // Manually prepare test environment for JsonSchema
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/jsonschema";

        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Copy all KSML files from resources to temp directory
        String[] jsonSchemaFiles = {"ksml-runner.yaml", "jsonschema-producer.yaml", "jsonschema-processor.yaml", "SensorData.json"};
        for (String fileName : jsonSchemaFiles) {
            Path sourcePath = Path.of(JsonSchemaRegistryIT.class.getResource(resourcePath + "/" + fileName).getPath());
            Path targetPath = tempDir.resolve(fileName);
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }

        // Update the runner config with local Kafka and Schema Registry URLs
        Path runnerTarget = tempDir.resolve("ksml-runner.yaml");
        String runnerContent = Files.readString(runnerTarget);
        runnerContent = runnerContent.replace("bootstrap.servers: broker:9093",
                                            "bootstrap.servers: " + kafka.getBootstrapServers());
        runnerContent = runnerContent.replace("apicurio.registry.url: http://schema-registry:8081/apis/registry/v2",
                                            "apicurio.registry.url: http://localhost:" + schemaRegistry.getMappedPort(8081) + "/apis/registry/v2");
        runnerContent = runnerContent.replace("storageDirectory: /ksml/state",
                                            "storageDirectory: " + stateDir);
        Files.writeString(runnerTarget, runnerContent);

        log.info("Using KSMLRunner directly with config: {}", runnerTarget);
        log.info("Apicurio Schema Registry URL: http://localhost:{}/apis/registry/v2", schemaRegistry.getMappedPort(8081));

        // Start KSML using KSMLRunner main method
        ksmlRunner = new KSMLRunnerWrapper(runnerTarget);
        ksmlRunner.start();

        // Wait for KSML to be ready
        waitForKSMLReady();
    }

    @Test
    void testKSMLJsonSchemaProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process JsonSchema sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertThat(ksmlRunner.isRunning()).isTrue().as("KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check sensor_data_jsonschema topic (producer output - JsonSchema data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-jsonschema");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_jsonschema"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).isNotEmpty().as("Should have generated sensor data in sensor_data_jsonschema topic");
            log.info("Found {} JsonSchema sensor messages", records.count());

            // Validate JsonSchema messages
            records.forEach(record -> {
                log.info("JsonSchema Sensor: key={}, value size={} bytes", record.key(), record.value().length());
                assertThat(record.key()).startsWith("sensor").as("Sensor key should start with 'sensor'");
                assertThat(record.value()).isNotEmpty().as("JsonSchema message should have content");
            });
        }

        // Check sensor_data_jsonschema_processed topic (processor output - transformed JSON)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_jsonschema_processed"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).isNotEmpty().as("Should have processed sensor data in sensor_data_jsonschema_processed topic");
            log.info("Found {} processed JSON messages", records.count());

            // Validate processed JSON structure and content using Jackson ObjectMapper and SoftAssertions
            SoftAssertions softly = new SoftAssertions();
            records.forEach(record -> {
                log.info("Processed JSON: key={}, value={}", record.key(), record.value());
                softly.assertThat(record.key()).startsWith("sensor").as("Sensor key should start with 'sensor'");

                // Use Jackson ObjectMapper for structured JSON validation
                JsonNode jsonNode = KSMLRunnerTestUtil.validateProcessedSensorJsonStructure(record.value(), softly);

                // Validate sensor type enum using JsonNode path access
                if (jsonNode != null) {
                    KSMLRunnerTestUtil.softAssertJsonEnumField(softly, jsonNode, "/type", "sensor type",
                        "AREA", "HUMIDITY", "LENGTH", "STATE", "TEMPERATURE");
                }
            });

            // Execute all soft assertions - will report ALL failures if any occur
            softly.assertAll();
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("JsonSchema Registry processing test completed successfully!");
        log.info("KSML generated JsonSchema sensor data using jsonschema-producer.yaml");
        log.info("KSML processed JsonSchema data and transformed to JSON using jsonschema-processor.yaml");
        log.info("JsonSchema validation and processing working correctly");
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSMLRunner to be ready...");

        // Use longer timeout for schema registry integration
        ksmlRunner.waitForReady(30000); // 30 seconds timeout for schema registry setup

        log.info("KSMLRunner is ready");
    }

    private void waitForSensorDataGeneration() throws InterruptedException {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 intervals
        // Since we can't check logs, wait for sufficient time for messages to be generated and processed
        Thread.sleep(10000); // Wait 10 seconds for messages to be generated and processed (longer for schema registry)

        log.info("Sensor data should have been generated by now");
    }

    private static void registerJsonSchema() throws InterruptedException {
        log.info("Registering JsonSchema with Apicurio Schema Registry...");

        // Read schema content from test resources
        String schemaPath = JsonSchemaRegistryIT.class.getResource("/docs-examples/beginner-tutorial/different-data-formats/jsonschema/SensorData.json").getPath();
        String schemaContent;
        try {
            schemaContent = Files.readString(Path.of(schemaPath));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read schema file: " + schemaPath, e);
        }

        // Wait for schema registry to be ready
        long startTime = System.currentTimeMillis();
        long timeout = 60000; // 60 seconds

        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                // Test if schema registry is ready
                String testUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081) + "/apis";
                var process = new ProcessBuilder("curl", "-s", testUrl).start();
                if (process.waitFor() == 0) {
                    log.info("Schema registry is ready");
                    break;
                }
            } catch (Exception e) {
                // Continue waiting
            }
            Thread.sleep(2000);
        }

        // Register schema using curl (similar to the docker-compose setup)
        try {
            String schemaRegistryUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081);

            // Escape the JSON schema content for the curl command
            String escapedSchema = schemaContent.replace("\"", "\\\"").replace("\n", "");
            String payload = "{\"schema\": \"" + escapedSchema + "\", \"schemaType\": \"JSON\"}";

            // Register for sensor_data_jsonschema topic
            String[] command1 = {
                "curl", "-X", "POST",
                "-H", "Content-Type: application/json",
                "-d", payload,
                schemaRegistryUrl + "/apis/ccompat/v7/subjects/sensor_data_jsonschema-value/versions?normalize=true"
            };

            ProcessBuilder pb1 = new ProcessBuilder(command1);
            Process process1 = pb1.start();
            int exitCode1 = process1.waitFor();

            // Register for sensor_data_jsonschema_processed topic
            String[] command2 = {
                "curl", "-X", "POST",
                "-H", "Content-Type: application/json",
                "-d", payload,
                schemaRegistryUrl + "/apis/ccompat/v7/subjects/sensor_data_jsonschema_processed-value/versions?normalize=true"
            };

            ProcessBuilder pb2 = new ProcessBuilder(command2);
            Process process2 = pb2.start();
            int exitCode2 = process2.waitFor();

            if (exitCode1 == 0 && exitCode2 == 0) {
                log.info("JsonSchema successfully registered for both topics");
            } else {
                log.warn("Schema registration may have failed: exitCode1={}, exitCode2={}", exitCode1, exitCode2);
            }

            // Wait a bit for registration to propagate
            Thread.sleep(2000);

        } catch (Exception e) {
            log.error("Failed to register JsonSchema", e);
            throw new RuntimeException("Failed to register JsonSchema", e);
        }
    }

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("sensor_data_jsonschema", 1, (short) 1),
                    new NewTopic("sensor_data_jsonschema_processed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: sensor_data_jsonschema, sensor_data_jsonschema_processed");
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