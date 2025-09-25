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
 * KSML Integration Test for XML data format processing.
 * This test validates that KSML can produce XML messages, transform them, and process them without schema registry.
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
        String resourcePath = "/docs-examples/beginner-tutorial/different-data-formats/xml";
        String runnerPath = XmlDataFormatIT.class.getResource(resourcePath + "/ksml-runner.yaml").getPath();
        String producerPath = XmlDataFormatIT.class.getResource(resourcePath + "/producer-xml.yaml").getPath();
        String processorPath = XmlDataFormatIT.class.getResource(resourcePath + "/processor-xml.yaml").getPath();
        String schemaPath = XmlDataFormatIT.class.getResource(resourcePath + "/SensorData.xsd").getPath();

        // Verify files exist
        File runnerFile = new File(runnerPath);
        File producerFile = new File(producerPath);
        File processorFile = new File(processorPath);
        File schemaFile = new File(schemaPath);

        if (!runnerFile.exists()) {
            throw new RuntimeException("Runner file not found: " + runnerPath);
        }
        if (!producerFile.exists()) {
            throw new RuntimeException("Producer file not found: " + producerPath);
        }
        if (!processorFile.exists()) {
            throw new RuntimeException("Processor file not found: " + processorPath);
        }
        if (!schemaFile.exists()) {
            throw new RuntimeException("Schema file not found: " + schemaPath);
        }

        log.info("Using KSML files from test resources:");
        log.info("  Runner: {}", runnerPath);
        log.info("  Producer: {}", producerPath);
        log.info("  Processor: {}", processorPath);
        log.info("  Schema: {}", schemaPath);

        // Start KSML container with file mounts
        ksmlContainer = new GenericContainer<>(DockerImageName.parse("registry.axual.io/opensource/images/axual/ksml:snapshot"))
                .withNetwork(network)
                .withNetworkAliases("ksml")
                .withWorkingDirectory("/ksml")
                .withCopyFileToContainer(MountableFile.forHostPath(runnerPath), "/ksml/ksml-runner.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(producerPath), "/ksml/producer-xml.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(processorPath), "/ksml/processor-xml.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(schemaPath), "/ksml/SensorData.xsd")
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
    @Timeout(90) // 1.5 minutes should be enough for XML processing
    void testKSMLXmlProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process XML sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertTrue(ksmlContainer.isRunning(), "KSML container should still be running");

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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have generated sensor data in ksml_sensordata_xml topic");
            log.info("Found {} XML sensor messages", records.count());

            // Validate XML messages
            records.forEach(record -> {
                log.info("XML Sensor: key={}, value={}", record.key(), record.value());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");

                // Validate XML structure - should contain XML tags
                String xmlValue = record.value();
                assertTrue(xmlValue.contains("<SensorData>"), "XML message should contain <SensorData> root element");
                assertTrue(xmlValue.contains("</SensorData>"), "XML message should contain </SensorData> closing tag");

                // XML messages should contain sensor data fields (as XML format)
                assertTrue(xmlValue.contains("<name>"), "XML message should contain <name> element");
                assertTrue(xmlValue.contains("<city>"), "XML message should contain <city> element");
                assertTrue(xmlValue.contains("<timestamp>"), "XML message should contain <timestamp> element");
                assertTrue(xmlValue.contains("<value>"), "XML message should contain <value> element");
                assertTrue(xmlValue.contains("<type>"), "XML message should contain <type> element");
                assertTrue(xmlValue.contains("<unit>"), "XML message should contain <unit> element");
                assertTrue(xmlValue.contains("<color>"), "XML message should contain <color> element");
                assertTrue(xmlValue.contains("<owner>"), "XML message should contain <owner> element");

                // Validate that XML is well-formed (has proper opening/closing tags)
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertEquals(openingTags, closingTags, "XML should have equal opening and closing angle brackets");
            });
        }

        // Check ksml_sensordata_xml_processed topic (processor output - transformed XML)
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml_processed"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertFalse(records.isEmpty(), "Should have processed sensor data in ksml_sensordata_xml_processed topic");
            log.info("Found {} processed XML messages", records.count());

            // Validate processed XML messages
            records.forEach(record -> {
                log.info("Processed XML: key={}, value={}", record.key(), record.value());
                assertTrue(record.key().startsWith("sensor"), "Sensor key should start with 'sensor'");

                // Validate XML structure contains processed data
                String xmlValue = record.value();
                assertTrue(xmlValue.contains("<SensorData>"), "Processed XML message should contain <SensorData> root element");
                assertTrue(xmlValue.contains("</SensorData>"), "Processed XML message should contain </SensorData> closing tag");

                // Validate that XML is well-formed
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertEquals(openingTags, closingTags, "Processed XML should have equal opening and closing angle brackets");
            });
        }

        // Check KSML logs for processing messages
        String logs = ksmlContainer.getLogs();
        assertTrue(logs.contains("Original: sensor=sensor"), "KSML should log original sensor processing");
        assertTrue(logs.contains("Transformed: sensor=sensor"), "KSML should log transformed sensor processing");

        // Verify specific city transformations in logs
        // The processor should uppercase city names from the original to transformed
        Map<String, String> expectedTransformations = Map.of(
            "Amsterdam", "AMSTERDAM",
            "Utrecht", "UTRECHT",
            "Rotterdam", "ROTTERDAM",
            "The Hague", "THE HAGUE",
            "Eindhoven", "EINDHOVEN"
        );

        // Check that we have at least some of the expected transformations in the logs
        int foundTransformations = 0;
        for (Map.Entry<String, String> entry : expectedTransformations.entrySet()) {
            String originalCity = entry.getKey();
            String transformedCity = entry.getValue();

            // Look for patterns like "Original: sensor=sensor0, city=Amsterdam"
            // followed by "Transformed: sensor=sensor0, city=AMSTERDAM"
            if (logs.contains("city=" + originalCity) && logs.contains("city=" + transformedCity)) {
                foundTransformations++;
                log.info("Found transformation: {} -> {}", originalCity, transformedCity);
            }
        }

        assertTrue(foundTransformations >= 1,
            "Should find at least 1 city transformation in logs, found: " + foundTransformations);

        // Verify the log pattern structure for original and transformed messages
        assertTrue(logs.matches("(?s).*Original: sensor=sensor\\d+, city=\\w+.*"),
            "Should have original sensor logs with pattern 'Original: sensor=sensorX, city=CITY'");
        assertTrue(logs.matches("(?s).*Transformed: sensor=sensor\\d+, city=[A-Z\\s]+.*"),
            "Should have transformed sensor logs with uppercase cities");

        // Verify that transformed cities are always uppercase
        String[] logLines = logs.split("\n");
        for (String line : logLines) {
            if (line.contains("Transformed: sensor=") && line.contains("city=")) {
                // Extract the city from the transformed log line
                int cityStart = line.indexOf("city=") + 5;
                if (cityStart > 4 && cityStart < line.length()) {
                    // Get the city value (everything after "city=" until end of line or next field)
                    String cityPart = line.substring(cityStart).trim();
                    // City name should be uppercase
                    if (!cityPart.isEmpty()) {
                        String city = cityPart.split(",")[0].trim(); // In case there are more fields
                        if (!city.isEmpty() && !city.equals(city.toUpperCase())) {
                            fail("Transformed city should be uppercase but found: " + city);
                        }
                    }
                }
            }
        }

        // Should not have errors
        assertFalse(logs.contains("ERROR"), "KSML should not have errors: " + extractErrors(logs));
        assertFalse(logs.contains("Exception"), "KSML should not have exceptions: " + extractErrors(logs));

        log.info("XML data format processing test completed successfully!");
        log.info("KSML generated XML sensor data using producer-xml.yaml");
        log.info("KSML processed XML data and transformed cities to uppercase using processor-xml.yaml");
        log.info("XML transformation and processing working correctly");
    }

    private static void waitForKSMLReady() throws InterruptedException {
        log.info("Waiting for KSML container to be ready...");

        // Wait for KSML to start processing - look for key log messages
        long startTime = System.currentTimeMillis();
        long timeout = 60000; // 60 seconds should be enough for simple XML setup
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

    private void waitForSensorDataGeneration() throws InterruptedException {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, wait for at least 2-3 messages to be generated
        // But check logs first to see if sensor data is already being generated
        String logs = ksmlContainer.getLogs();
        if (logs.contains("Transformed: sensor=sensor")) {
            log.info("Sensor data already being generated");
            return;
        }

        // Wait up to 15 seconds for first sensor data (3s interval + some buffer)
        long startTime = System.currentTimeMillis();
        long timeout = 15000; // 15 seconds should be enough

        while (System.currentTimeMillis() - startTime < timeout) {
            logs = ksmlContainer.getLogs();
            if (logs.contains("Transformed: sensor=sensor")) {
                log.info("Sensor data generation detected");
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
                    new NewTopic("ksml_sensordata_xml", 1, (short) 1),
                    new NewTopic("ksml_sensordata_xml_processed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get();
            log.info("Created topics: ksml_sensordata_xml, ksml_sensordata_xml_processed");
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