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
import org.junit.jupiter.api.Timeout;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KSML Integration Test with Apicurio AVRO and Apicurio Schema Registry that tests AVRO processing.
 * This test validates that KSML can produce AVRO messages and convert them to JSON using apicurio_avro notation.
 *
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class ApicurioAvroSchemaRegistryIT {

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
            .withEnv("APICURIO_STORAGE_KIND", "kafkasql")
            .withEnv("APICURIO_KAFKASQL_BOOTSTRAP_SERVERS", "broker:9093")
            .withEnv("APICURIO_KAFKASQL_TOPIC", "_apicurio-kafkasql-store")
            .withEnv("APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED", "true")
            .waitingFor(Wait.forHttp("/apis").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)))
            .dependsOn(kafka);

    static KSMLRunnerWrapper ksmlRunner;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        // Create topics first
        createTopics();

        // Manually prepare test environment for Apicurio Avro (due to subdirectory structure)
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/avro";

        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Copy all KSML files from resources to temp directory
        String[] avroFiles = {"producer-avro.yaml", "processor-avro-convert.yaml", "SensorData.avsc"};
        for (String fileName : avroFiles) {
            Path sourcePath = Path.of(ApicurioAvroSchemaRegistryIT.class.getResource(resourcePath + "/" + fileName).getPath());
            Path targetPath = tempDir.resolve(fileName);
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }

        // Copy the apicurio-specific ksml-runner.yaml
        Path apicurioRunnerSource = Path.of(ApicurioAvroSchemaRegistryIT.class.getResource(resourcePath + "/apicurio_avro/ksml-runner.yaml").getPath());
        Path runnerTarget = tempDir.resolve("ksml-runner.yaml");
        Files.copy(apicurioRunnerSource, runnerTarget, StandardCopyOption.REPLACE_EXISTING);

        // Update the runner config with local Kafka and Schema Registry URLs
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
    @Timeout(150) // 2.5 minutes to account for schema registry startup time
    void testKSMLApicurioAvroToJsonConversion() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlRunner.isRunning(), "KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check sensor_data_avro topic (producer output - AVRO data serialized as bytes)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-apicurio-avro");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_avro"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated sensor data in sensor_data_avro topic");
            log.info("Found {} Apicurio AVRO sensor messages", records.count());

            // Log some sample sensor data keys (values will be binary AVRO)
            records.forEach(record -> {
                log.info("🔬 Apicurio AVRO Sensor: key={}, value size={} bytes", record.key(), record.value().length());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");
            });
        }

        // Check sensor_data_json topic (processor output - converted JSON)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-apicurio-json");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_json"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have converted sensor data in sensor_data_json topic");
            log.info("Found {} JSON sensor messages", records.count());

            // Validate JSON structure and content
            records.forEach(record -> {
                log.info("JSON Sensor: key={}, value={}", record.key(), record.value());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");

                // Validate JSON structure contains expected sensor data fields
                String jsonValue = record.value();
                assertTrue(jsonValue.contains("\"name\""), "JSON should contain name field");
                assertTrue(jsonValue.contains("\"timestamp\""), "JSON should contain timestamp field");
                assertTrue(jsonValue.contains("\"value\""), "JSON should contain value field");
                assertTrue(jsonValue.contains("\"type\""), "JSON should contain type field");
                assertTrue(jsonValue.contains("\"unit\""), "JSON should contain unit field");

                // Check that sensor type is one of the valid enum values
                boolean hasValidType = jsonValue.contains("\"AREA\"") ||
                                     jsonValue.contains("\"HUMIDITY\"") ||
                                     jsonValue.contains("\"LENGTH\"") ||
                                     jsonValue.contains("\"STATE\"") ||
                                     jsonValue.contains("\"TEMPERATURE\"");
                assertTrue(hasValidType, "JSON should contain valid sensor type enum");
            });
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("Apicurio AVRO Schema Registry AVRO to JSON conversion test completed successfully!");
        log.info("KSML generated AVRO sensor data using producer-avro.yaml with apicurio_avro");
        log.info("KSML converted AVRO to JSON using processor-avro-convert.yaml with apicurio_avro");
        log.info("Apicurio schema registry integration working correctly");
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

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("sensor_data_avro", 1, (short) 1),
                    new NewTopic("sensor_data_json", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: sensor_data_avro, sensor_data_json");
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