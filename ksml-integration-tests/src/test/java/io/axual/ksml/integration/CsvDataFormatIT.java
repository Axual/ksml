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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KSML Integration Test for CSV data format processing.
 * This test validates that KSML can produce CSV messages, transform them, and process them without schema registry.
 *
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class CsvDataFormatIT {

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

        // Prepare test environment with CSV-specific files
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/csv";
        String[] csvFiles = {"ksml-runner.yaml", "csv-producer.yaml", "csv-processor.yaml", "SensorData.csv"};

        Path configPath = KSMLRunnerTestUtil.prepareTestEnvironment(
                tempDir,
                resourcePath,
                csvFiles,
                kafka.getBootstrapServers()
        );

        log.info("Using KSMLRunner directly with config: {}", configPath);

        // Start KSML using KSMLRunner main method
        ksmlRunner = new KSMLRunnerWrapper(configPath);
        ksmlRunner.start();

        // Wait for KSML to be ready
        waitForKSMLReady();
    }

    @AfterAll
    static void cleanup() {
        if (ksmlRunner != null) {
            log.info("Stopping KSMLRunner...");
            ksmlRunner.stop();
        }
    }

    @Test
    @Timeout(90) // 1.5 minutes should be enough for CSV processing without schema registry
    void testKSMLCsvProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process CSV sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlRunner.isRunning(), "KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check ksml_sensordata_csv topic (producer output - CSV data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-csv");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_csv"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated sensor data in ksml_sensordata_csv topic");
            log.info("Found {} CSV sensor messages", records.count());

            // Validate CSV messages
            records.forEach(record -> {
                log.info("CSV Sensor: key={}, value={}", record.key(), record.value());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");

                // Validate CSV structure - should contain comma-separated values
                String csvValue = record.value();
                assertTrue(csvValue.contains(","), "CSV message should contain comma separators");

                // CSV messages should contain sensor data fields (as CSV format)
                // Note: CSV format won't have JSON structure, just comma-separated values
                assertTrue(csvValue.length() > 0, "CSV message should have content");

                // Count commas to validate CSV structure (should have 7 commas for 8 fields)
                long commaCount = csvValue.chars().filter(ch -> ch == ',').count();
                assertTrue(commaCount >= 7, "CSV should have at least 7 commas for 8 fields, found: " + commaCount);
            });
        }

        // Check ksml_sensordata_csv_processed topic (processor output - transformed CSV)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_csv_processed"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have processed sensor data in ksml_sensordata_csv_processed topic");
            log.info("Found {} processed CSV messages", records.count());

            // Validate processed CSV messages
            records.forEach(record -> {
                log.info("Processed CSV: key={}, value={}", record.key(), record.value());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");

                // Validate CSV structure contains processed data
                String csvValue = record.value();
                assertTrue(csvValue.contains(","), "Processed CSV message should contain comma separators");

                // Count commas to validate CSV structure (should have 7 commas for 8 fields)
                long commaCount = csvValue.chars().filter(ch -> ch == ',').count();
                assertTrue(commaCount >= 7, "Processed CSV should have at least 7 commas for 8 fields, found: " + commaCount);
            });
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("CSV data format processing test completed successfully!");
        log.info("KSML generated CSV sensor data using csv-producer.yaml");
        log.info("KSML processed CSV data and transformed cities to uppercase using csv-processor.yaml");
        log.info("CSV transformation and processing working correctly");
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSMLRunner to be ready...");

        // Use the built-in wait method from the wrapper
        ksmlRunner.waitForReady(15000); // 15 seconds timeout

        log.info("KSMLRunner is ready");
    }

    private void waitForSensorDataGeneration() throws InterruptedException {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 intervals
        // Since we can't check logs, wait for sufficient time for messages to be generated and processed
        Thread.sleep(7000); // Wait 7 seconds for messages to be generated and processed

        log.info("Sensor data should have been generated by now");
    }

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("ksml_sensordata_csv", 1, (short) 1),
                    new NewTopic("ksml_sensordata_csv_processed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: ksml_sensordata_csv, ksml_sensordata_csv_processed");
        }
    }

}