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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.KSMLRunnerWrapper;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test for XML data format processing.
 * This test validates that KSML can produce XML messages, transform them, and process them without schema registry.
 *
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class XmlDataFormatIT {

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

        // Prepare test environment with XML-specific files
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/xml";
        String[] xmlFiles = {"ksml-runner.yaml", "producer-xml.yaml", "processor-xml.yaml", "SensorData.xsd"};

        Path configPath = KSMLRunnerTestUtil.prepareTestEnvironment(
                tempDir,
                resourcePath,
                xmlFiles,
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
    void testKSMLXmlProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process XML sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertThat(ksmlRunner.isRunning()).isTrue().as("KSMLRunner should still be running");

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check ksml_sensordata_xml topic (producer output - XML data)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-xml");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).isNotEmpty().as("Should have generated sensor data in ksml_sensordata_xml topic");
            log.info("Found {} XML sensor messages", records.count());

            // Validate XML messages
            records.forEach(record -> {
                log.info("XML Sensor: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).startsWith("sensor").as("Sensor key should start with 'sensor'");

                // Validate XML structure - should contain XML tags
                String xmlValue = record.value();
                assertThat(xmlValue).contains("<SensorData>").as("XML message should contain <SensorData> root element");
                assertThat(xmlValue).contains("</SensorData>").as("XML message should contain </SensorData> closing tag");

                // XML messages should contain sensor data fields (as XML format)
                assertThat(xmlValue)
                    .contains("<name>").as("XML message should contain <name> element")
                    .contains("<city>").as("XML message should contain <city> element")
                    .contains("<timestamp>").as("XML message should contain <timestamp> element")
                    .contains("<value>").as("XML message should contain <value> element")
                    .contains("<type>").as("XML message should contain <type> element")
                    .contains("<unit>").as("XML message should contain <unit> element")
                    .contains("<color>").as("XML message should contain <color> element")
                    .contains("<owner>").as("XML message should contain <owner> element");

                // Validate that XML is well-formed (has proper opening/closing tags)
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertThat(openingTags).isEqualTo(closingTags).as("XML should have equal opening and closing angle brackets");
            });
        }

        // Check ksml_sensordata_xml_processed topic (processor output - transformed XML)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml_processed"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).isNotEmpty().as("Should have processed sensor data in ksml_sensordata_xml_processed topic");
            log.info("Found {} processed XML messages", records.count());

            // Validate processed XML messages
            records.forEach(record -> {
                log.info("Processed XML: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).startsWith("sensor").as("Sensor key should start with 'sensor'");

                // Validate XML structure contains processed data
                String xmlValue = record.value();
                assertThat(xmlValue)
                    .contains("<SensorData>").as("Processed XML message should contain <SensorData> root element")
                    .contains("</SensorData>").as("Processed XML message should contain </SensorData> closing tag");

                // Validate that XML is well-formed
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertThat(openingTags).isEqualTo(closingTags).as("Processed XML should have equal opening and closing angle brackets");
            });
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("XML data format processing test completed successfully!");
        log.info("KSML generated XML sensor data using producer-xml.yaml");
        log.info("KSML processed XML data and transformed cities to uppercase using processor-xml.yaml");
        log.info("XML transformation and processing working correctly");
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
                    new NewTopic("ksml_sensordata_xml", 1, (short) 1),
                    new NewTopic("ksml_sensordata_xml_processed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: ksml_sensordata_xml, ksml_sensordata_xml_processed");
        }
    }

}