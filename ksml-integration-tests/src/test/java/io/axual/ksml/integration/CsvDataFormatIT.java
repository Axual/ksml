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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
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
 * KSML Integration Test for CSV data format processing.
 * This test validates that KSML can produce CSV messages, transform them, and process them without schema registry.
 * <p>
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

    @Container
    static KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/csv",
                          "ksml-runner.yaml", "csv-producer.yaml", "csv-processor.yaml", "SensorData.csv")
            .withKafka(kafka)
            .withTopics("ksml_sensordata_csv", "ksml_sensordata_csv_processed")
            .dependsOn(kafka);

    @Test
    void testKSMLCsvProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process CSV sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Create consumer properties
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check ksml_sensordata_csv topic (producer output - CSV data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-csv");
        Map<String, String> originalMessages = new LinkedHashMap<>(); // Use LinkedHashMap to preserve insertion order
        List<String> originalMessageOrder = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_csv"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated sensor data in ksml_sensordata_csv topic").isNotEmpty();
            log.info("Found {} CSV sensor messages", records.count());

            // Validate CSV messages and store for comparison
            records.forEach(record -> {
                log.info("CSV Sensor: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                // Validate CSV structure - should contain comma-separated values
                String csvValue = record.value();
                assertThat(csvValue).as("CSV message should contain comma separators").contains(",");

                // Count commas to validate CSV structure (should have 7 commas for 8 fields)
                // Schema: name,timestamp,value,type,unit,color,city,owner
                long commaCount = csvValue.chars().filter(ch -> ch == ',').count();
                assertThat(commaCount).as("CSV should have exactly 7 commas for 8 fields, found: " + commaCount).isEqualTo(7);

                // Validate expected CSV fields exist (basic structure check)
                String[] fields = csvValue.split(",", -1);
                assertThat(fields).as("CSV should have exactly 8 fields").hasSize(8);

                // Store original for comparison with processed version (preserving order)
                originalMessages.put(record.key(), csvValue);
                originalMessageOrder.add(record.key());
            });
        }

        // Check ksml_sensordata_csv_processed topic (processor output - transformed CSV)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        List<String> processedMessageOrder = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_csv_processed"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have processed sensor data in ksml_sensordata_csv_processed topic").isNotEmpty();
            log.info("Found {} processed CSV messages", records.count());

            // Validate processed CSV messages against originals
            records.forEach(record -> {
                // Track processing order
                processedMessageOrder.add(record.key());
                log.info("Processed CSV: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                // Validate CSV structure contains processed data
                String processedCsvValue = record.value();
                assertThat(processedCsvValue).as("Processed CSV message should contain comma separators").contains(",");

                // Count commas to validate CSV structure (should have 7 commas for 8 fields)
                long commaCount = processedCsvValue.chars().filter(ch -> ch == ',').count();
                assertThat(commaCount).as("Processed CSV should have exactly 7 commas for 8 fields, found: " + commaCount).isEqualTo(7);

                // Parse processed CSV fields
                String[] processedFields = processedCsvValue.split(",", -1);
                assertThat(processedFields).as("Processed CSV should have exactly 8 fields").hasSize(8);

                // Verify transformation: city should be uppercase
                String originalCsvValue = originalMessages.get(record.key());
                assertThat(originalCsvValue).as("Should have original message for key: " + record.key()).isNotNull();

                String[] originalFields = originalCsvValue.split(",", -1);

                // Schema: name,timestamp,value,type,unit,color,city,owner (city is index 6)
                String originalCity = originalFields[6];
                String processedCity = processedFields[6];

                assertThat(processedCity).isEqualTo(originalCity.toUpperCase())
                    .as("City should be uppercase: original='%s', processed='%s'", originalCity, processedCity);

                // Verify other fields remain unchanged
                for (int i = 0; i < originalFields.length; i++) {
                    if (i != 6) { // Skip city field (index 6)
                        assertThat(processedFields[i]).isEqualTo(originalFields[i])
                            .as("Field %d should remain unchanged: original='%s', processed='%s'", i, originalFields[i], processedFields[i]);
                    }
                }
            });

            // Verify that processed messages maintain the same order as original messages
            assertThat(processedMessageOrder)
                .as("Processed messages should maintain the same order as original messages")
                .isEqualTo(originalMessageOrder);
            log.info("Message ordering verified: {} messages processed in correct order", processedMessageOrder.size());
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("CSV data format processing test completed successfully!");
        log.info("KSML generated CSV sensor data using csv-producer.yaml");
        log.info("KSML processed CSV data and transformed cities to uppercase using csv-processor.yaml");
        log.info("CSV transformation and processing working correctly");
    }

    private void waitForSensorDataGeneration() throws Exception {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 messages
        // Use AdminClient to check actual message count instead of fixed sleep
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "ksml_sensordata_csv",
            2, // Wait for at least 2 messages
            Duration.ofSeconds(30) // Maximum 30 seconds (much better than fixed 7s)
        );

        log.info("Sensor data has been generated and verified");
    }

}