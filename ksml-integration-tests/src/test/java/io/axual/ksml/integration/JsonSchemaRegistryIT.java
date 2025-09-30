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

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.KSMLRunnerWrapper;
import io.axual.ksml.integration.testutil.SensorDataTestUtil;
import lombok.extern.slf4j.Slf4j;

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

        // Prepare test environment using utility method
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/jsonschema";
        String[] jsonSchemaFiles = {"ksml-runner.yaml", "jsonschema-producer.yaml", "jsonschema-processor.yaml", "SensorData.json"};

        KSMLRunnerTestUtil.SchemaRegistryConfig schemaRegistryConfig = new KSMLRunnerTestUtil.SchemaRegistryConfig(
            "apicurio.registry.url: http://schema-registry:8081/apis/registry/v2",
            "apicurio.registry.url: http://localhost:" + schemaRegistry.getMappedPort(8081) + "/apis/registry/v2"
        );

        Path configPath = KSMLRunnerTestUtil.prepareTestEnvironment(
            tempDir,
            resourcePath,
            jsonSchemaFiles,
            kafka.getBootstrapServers(),
            null, // no subdirectory needed
            schemaRegistryConfig
        );

        log.info("Using KSMLRunner directly with config: {}", configPath);
        log.info("Apicurio Schema Registry URL: http://localhost:{}/apis/registry/v2", schemaRegistry.getMappedPort(8081));

        // Start KSML using KSMLRunner main method
        ksmlRunner = new KSMLRunnerWrapper(configPath);
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
                JsonNode jsonNode = SensorDataTestUtil.validateProcessedSensorJsonStructure(record.value(), softly);

                // Validate sensor type enum using JsonNode path access
                if (jsonNode != null) {
                    SensorDataTestUtil.softAssertJsonEnumField(softly, jsonNode, "/type", "sensor type",
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

    private void waitForSensorDataGeneration() throws Exception {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 messages
        // Use AdminClient to check actual message count instead of fixed sleep
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "sensor_data_jsonschema",
            2, // Wait for at least 2 messages
            Duration.ofSeconds(30) // Maximum 30 seconds (much better than fixed 10s)
        );

        log.info("Sensor data has been generated and verified");
    }

    private static void registerJsonSchema() throws Exception {
        log.info("Registering JsonSchema with Apicurio Schema Registry...");

        // Read schema content from test resources
        String schemaPath = JsonSchemaRegistryIT.class.getResource("/docs-examples/beginner-tutorial/different-data-formats/jsonschema/SensorData.json").getPath();
        String schemaContent;
        try {
            schemaContent = Files.readString(Path.of(schemaPath));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read schema file: " + schemaPath, e);
        }

        String schemaRegistryUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081);

        // Wait for schema registry to be ready using HTTP client
        KSMLRunnerTestUtil.waitForSchemaRegistryReady(schemaRegistryUrl, Duration.ofMinutes(1));

        // Register schema for both topics using HTTP client
        try {
            // Register for sensor_data_jsonschema topic
            KSMLRunnerTestUtil.registerJsonSchema(schemaRegistryUrl, "sensor_data_jsonschema-value", schemaContent);

            // Register for sensor_data_jsonschema_processed topic
            KSMLRunnerTestUtil.registerJsonSchema(schemaRegistryUrl, "sensor_data_jsonschema_processed-value", schemaContent);

            log.info("JsonSchema successfully registered for both topics");

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