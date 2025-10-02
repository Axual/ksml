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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import io.axual.ksml.integration.testutil.ApicurioSchemaRegistryContainer;
import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.SensorDataTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test with Apicurio AVRO and Apicurio Schema Registry that tests AVRO processing.
 * This test validates that KSML can produce AVRO messages and convert them to JSON using apicurio_avro notation.
 * <p>
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class ApicurioAvroSchemaRegistryIT {

    static final Network network = Network.newNetwork();

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static final ApicurioSchemaRegistryContainer schemaRegistry = new ApicurioSchemaRegistryContainer()
            .withNetwork(network)
            .withLegacyIdMode();

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro",
                          "producer-avro.yaml", "processor-avro-convert.yaml", "SensorData.avsc")
            .withKafka(kafka)
            .withApicurioAvroRegistry(schemaRegistry)
            .withTopics("sensor_data_avro", "sensor_data_avro_processed")
            .dependsOn(kafka, schemaRegistry);


    @Test
    void testKSMLApicurioAvroToJsonConversion() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Create consumer properties
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check sensor_data_avro topic (producer output - AVRO data serialized as bytes)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-apicurio-avro");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_avro"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated sensor data in sensor_data_avro topic").isNotEmpty();
            log.info("Found {} Apicurio AVRO sensor messages", records.count());

            // Log some sample sensor data keys (values will be binary AVRO)
            records.forEach(record -> {
                log.info("🔬 Apicurio AVRO Sensor: key={}, value size={} bytes", record.key(), record.value().length());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");
            });
        }

        // Check sensor_data_json topic (processor output - converted JSON)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-apicurio-json");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("sensor_data_json"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have converted sensor data in sensor_data_json topic").isNotEmpty();
            log.info("Found {} JSON sensor messages", records.count());

            // Validate JSON structure and content using Jackson ObjectMapper and SoftAssertions
            final SoftAssertions softly = new SoftAssertions();
            records.forEach(record -> {
                log.info("JSON Sensor: key={}, value={}", record.key(), record.value());
                softly.assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                // Use Jackson ObjectMapper for structured JSON validation
                final JsonNode jsonNode = SensorDataTestUtil.validateSensorJsonStructure(record.value(), softly);

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

        log.info("Apicurio AVRO Schema Registry AVRO to JSON conversion test completed successfully!");
        log.info("KSML generated AVRO sensor data using producer-avro.yaml with apicurio_avro");
        log.info("KSML converted AVRO to JSON using processor-avro-convert.yaml with apicurio_avro");
        log.info("Apicurio schema registry integration working correctly");
    }

    private void waitForSensorDataGeneration() throws Exception {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 messages
        // Use AdminClient to check actual message count
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "sensor_data_avro",
            2, // Wait for at least 2 messages
            Duration.ofSeconds(30) // Maximum 30 seconds
        );

        log.info("Sensor data has been generated and verified");
    }

}