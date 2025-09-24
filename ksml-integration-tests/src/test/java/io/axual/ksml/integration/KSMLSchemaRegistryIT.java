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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
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
 * KSML Integration Test with Apicurio Schema Registry that tests AVRO to JSON conversion.
 * This test mimics the docker-compose-setup-with-sr configuration and validates
 * that KSML can produce AVRO messages and convert them to JSON using schema registry.
 */
@Slf4j
@Testcontainers
class KSMLSchemaRegistryIT {

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
            .waitingFor(Wait.forHttp("/apis").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)))
            .dependsOn(kafka);

    static GenericContainer<?> ksmlContainer;

    @BeforeAll
    static void setup() throws Exception {
        // Create topics first
        createTopics();

        // Create temporary directory for KSML configuration
        Path tempDir = Files.createTempDirectory("ksml-schema-registry-test");
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Create ksml-runner.yaml with schema registry configuration
        String runnerYaml = """
            ksml:
              definitions:
                producer: producer-avro.yaml
                processor: processor-avro-convert.yaml
              schemaRegistries:
                my_schema_registry:
                  config:
                    schema.registry.url: http://schema-registry:8081/apis/ccompat/v7
              notations:
                avro:
                  type: confluent_avro
                  schemaRegistry: my_schema_registry
                  config:
                    normalize.schemas: true
                    auto.register.schemas: true
              storageDirectory: /ksml/state
              createStorageDirectory: true

            kafka:
              bootstrap.servers: broker:9093
              application.id: io.ksml.schema.registry.test
              security.protocol: PLAINTEXT
              acks: all
            """;

        Files.writeString(tempDir.resolve("ksml-runner.yaml"), runnerYaml);

        // Get the actual KSML definition files
        String producerPath = "/Users/km/dev/ksml/docs/definitions/beginner-tutorial/different-data-formats/producer-avro.yaml";
        String processorPath = "/Users/km/dev/ksml/docs/definitions/beginner-tutorial/different-data-formats/processor-avro-convert.yaml";
        String schemaPath = "/Users/km/dev/ksml/docs/local-docker-compose-setup-with-sr/examples/SensorData.avsc";

        // Verify files exist
        File producerFile = new File(producerPath);
        File processorFile = new File(processorPath);
        File schemaFile = new File(schemaPath);

        if (!producerFile.exists()) {
            throw new RuntimeException("Producer file not found: " + producerPath);
        }
        if (!processorFile.exists()) {
            throw new RuntimeException("Processor file not found: " + processorPath);
        }
        if (!schemaFile.exists()) {
            throw new RuntimeException("Schema file not found: " + schemaPath);
        }

        log.info("Using KSML files:");
        log.info("  Producer: {}", producerPath);
        log.info("  Processor: {}", processorPath);
        log.info("  Schema: {}", schemaPath);

        // Start KSML container with file mounts
        ksmlContainer = new GenericContainer<>(DockerImageName.parse("registry.axual.io/opensource/images/axual/ksml:1.1.0"))
                .withNetwork(network)
                .withNetworkAliases("ksml")
                .withWorkingDirectory("/ksml")
                .withCopyFileToContainer(MountableFile.forHostPath(tempDir.resolve("ksml-runner.yaml").toString()), "/ksml/ksml-runner.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(producerPath), "/ksml/producer-avro.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(processorPath), "/ksml/processor-avro-convert.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(schemaPath), "/ksml/SensorData.avsc")
                .withCopyFileToContainer(MountableFile.forHostPath(stateDir.toString()), "/ksml/state")
                .withCommand("ksml-runner.yaml")
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("KSML"))
                .dependsOn(kafka, schemaRegistry);

        log.info("Starting KSML container...");
        ksmlContainer.start();

        // Wait for KSML to be ready by checking logs
        waitForKSMLReady();
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSML container to be ready...");

        // Wait for KSML to start processing - look for key log messages
        long startTime = System.currentTimeMillis();
        long timeout = 90000; // 90 seconds max (schema registry takes longer to initialize)
        boolean ksmlReady = false;
        boolean producerStarted = false;

        while (System.currentTimeMillis() - startTime < timeout) {
            // Check if container is still running
            if (!ksmlContainer.isRunning()) {
                String logs = ksmlContainer.getLogs();
                log.error("KSML container exited. Logs:\\n{}", logs);
                throw new RuntimeException("KSML container exited with logs:\\n" + logs);
            }

            String logs = ksmlContainer.getLogs();

            // Look for KSML processing ready state
            if (!ksmlReady && logs.contains("Pipeline processing state change. Moving from old state 'REBALANCING' to new state 'RUNNING'")) {
                log.info("✅ KSML pipeline is running");
                ksmlReady = true;
            }

            // Look for producer started
            if (!producerStarted && logs.contains("Starting Kafka producer(s)")) {
                log.info("✅ KSML producer started");
                producerStarted = true;
            }

            // Both conditions met
            if (ksmlReady && producerStarted) {
                log.info("✅ KSML container fully ready");
                return;
            }

            Thread.sleep(1000); // Check every second
        }

        throw new RuntimeException("KSML did not become ready within " + (timeout / 1000) + " seconds");
    }

    private void waitForSensorDataGeneration() throws InterruptedException {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, wait for at least 2-3 messages to be generated
        // But check logs first to see if sensor data is already being generated
        String logs = ksmlContainer.getLogs();
        if (logs.contains("Original Avro: sensor=")) {
            log.info("✅ Sensor data already being generated");
            return;
        }

        // Wait up to 15 seconds for first sensor data (3s interval + some buffer)
        long startTime = System.currentTimeMillis();
        long timeout = 15000; // 15 seconds should be enough

        while (System.currentTimeMillis() - startTime < timeout) {
            logs = ksmlContainer.getLogs();
            if (logs.contains("Original Avro: sensor=")) {
                log.info("✅ Sensor data generation detected");
                // Wait for one more interval to ensure processing
                Thread.sleep(4000); // 3s interval + 1s buffer
                return;
            }
            Thread.sleep(1000);
        }

        throw new RuntimeException("No sensor data generation detected within " + (timeout / 1000) + " seconds");
    }

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("sensor_data_avro", 1, (short) 1),
                    new NewTopic("sensor_data_json", 1, (short) 1),
                    new NewTopic("combined_sensor_data", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("✅ Created topics: sensor_data_avro, sensor_data_json, combined_sensor_data");
        }
    }

    @Test
    @Timeout(150) // 2.5 minutes to account for schema registry startup time
    void testKSMLAvroToJsonConversion() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlContainer.isRunning(), "KSML container should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check sensor_data_avro topic (producer output - AVRO data serialized as bytes)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-avro");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_avro"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated sensor data in sensor_data_avro topic");
            log.info("✅ Found {} AVRO sensor messages", records.count());

            // Log some sample sensor data keys (values will be binary AVRO)
            records.forEach(record -> {
                log.info("🔬 AVRO Sensor: key={}, value size={} bytes", record.key(), record.value().length());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");
            });
        }

        // Check sensor_data_json topic (processor output - converted JSON)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-json");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_json"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have converted sensor data in sensor_data_json topic");
            log.info("✅ Found {} JSON sensor messages", records.count());

            // Validate JSON structure and content
            records.forEach(record -> {
                log.info("📊 JSON Sensor: key={}, value={}", record.key(), record.value());
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

        // Check KSML logs for processing messages
        String logs = ksmlContainer.getLogs();
        assertTrue(logs.contains("Original Avro: sensor="), "KSML should log original AVRO processing");
        assertTrue(logs.contains("Converted to JSON: sensor="), "KSML should log JSON conversion");

        // Should not have errors
        assertFalse(logs.contains("ERROR"), "KSML should not have errors: " + extractErrors(logs));
        assertFalse(logs.contains("Exception"), "KSML should not have exceptions: " + extractErrors(logs));

        log.info("🎉 KSML Schema Registry AVRO to JSON conversion test completed successfully!");
        log.info("✅ KSML container executed real YAML definitions with schema registry");
        log.info("✅ KSML generated AVRO sensor data using producer-avro.yaml");
        log.info("✅ KSML converted AVRO to JSON using processor-avro-convert.yaml");
        log.info("✅ Schema registry integration working correctly");
    }

    private String extractErrors(String logs) {
        return logs.lines()
                .filter(line -> line.contains("ERROR") || line.contains("Exception"))
                .limit(5)
                .reduce((a, b) -> a + "\\n" + b)
                .orElse("No specific errors found");
    }
}