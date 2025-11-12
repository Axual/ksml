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

import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test for XML data format processing.
 * This test validates that KSML can produce XML messages, transform them, and process them without schema registry.
 * <p>
 * This test runs KSMLRunner directly using its main method instead of using a Docker container.
 */
@Slf4j
@Testcontainers
class XmlDataFormatIT {

    static final Network network = Network.newNetwork();

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/xml",
                          "ksml-runner.yaml", "producer-xml.yaml", "processor-xml.yaml", "SensorData.xsd")
            .withKafka(kafka)
            .withTopics("ksml_sensordata_xml", "ksml_sensordata_xml_processed")
            .dependsOn(kafka);

    @Test
    void testKSMLXmlProcessing() throws Exception {
        // Wait for first sensor data to be generated and processed
        log.info("Waiting for KSML to generate and process XML sensor data...");
        waitForSensorDataGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Check ksml_sensordata_xml topic (producer output - XML data)
        final Properties consumerProps = createConsumerProperties("test-consumer-xml");
        Map<String, String> originalMessages = new LinkedHashMap<>(); // Store original XML for comparison
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated sensor data in ksml_sensordata_xml topic").isNotEmpty();
            log.info("Found {} XML sensor messages", records.count());

            // Validate XML messages using proper DOM parsing
            records.forEach(record -> {
                log.info("XML Sensor: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                String xmlValue = record.value();
                try {
                    // Parse XML using DOM parser - this validates well-formedness
                    Document doc = parseXmlDocument(xmlValue);

                    // Verify root element
                    Element root = doc.getDocumentElement();
                    assertThat(root.getTagName()).as("Root element should be SensorData").isEqualTo("SensorData");

                    // Validate all required sensor data fields exist and are non-empty
                    String name = getElementTextContent(doc, "name");
                    String timestamp = getElementTextContent(doc, "timestamp");
                    String value = getElementTextContent(doc, "value");
                    String type = getElementTextContent(doc, "type");
                    String unit = getElementTextContent(doc, "unit");
                    String color = getElementTextContent(doc, "color");
                    String city = getElementTextContent(doc, "city");
                    String owner = getElementTextContent(doc, "owner");

                    assertThat(name).as("Name field should not be empty").isNotEmpty();
                    assertThat(timestamp).as("Timestamp field should not be empty").isNotEmpty();
                    assertThat(value).as("Value field should not be empty").isNotEmpty();
                    assertThat(type).as("Type field should not be empty").isNotEmpty();
                    assertThat(unit).as("Unit field should not be empty").isNotEmpty();
                    assertThat(color).as("Color field should not be empty").isNotEmpty();
                    assertThat(city).as("City field should not be empty").isNotEmpty();
                    assertThat(owner).as("Owner field should not be empty").isNotEmpty();

                    log.info("Parsed XML fields: name={}, timestamp={}, value={}, type={}, unit={}, color={}, city={}, owner={}",
                        name, timestamp, value, type, unit, color, city, owner);

                    // Store original for comparison with processed version
                    originalMessages.put(record.key(), xmlValue);
                } catch (Exception e) {
                    throw new AssertionError("Failed to parse XML: " + xmlValue, e);
                }
            });
        }

        // Check ksml_sensordata_xml_processed topic (processor output - transformed XML)
        final Properties processedConsumerProps = createConsumerProperties("test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(processedConsumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml_processed"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have processed sensor data in ksml_sensordata_xml_processed topic").isNotEmpty();
            log.info("Found {} processed XML messages", records.count());

            // Validate processed XML messages against originals using proper DOM parsing
            records.forEach(record -> {
                log.info("Processed XML: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                String processedXmlValue = record.value();
                String originalXmlValue = originalMessages.get(record.key());
                assertThat(originalXmlValue).as("Should have original message for key: " + record.key()).isNotNull();

                try {
                    // Parse both original and processed XML using DOM parser
                    Document processedDoc = parseXmlDocument(processedXmlValue);
                    Document originalDoc = parseXmlDocument(originalXmlValue);

                    // Verify root element
                    Element processedRoot = processedDoc.getDocumentElement();
                    assertThat(processedRoot.getTagName()).as("Processed root element should be SensorData").isEqualTo("SensorData");

                    // Extract all fields from processed XML
                    String processedName = getElementTextContent(processedDoc, "name");
                    String processedTimestamp = getElementTextContent(processedDoc, "timestamp");
                    String processedValue = getElementTextContent(processedDoc, "value");
                    String processedType = getElementTextContent(processedDoc, "type");
                    String processedUnit = getElementTextContent(processedDoc, "unit");
                    String processedColor = getElementTextContent(processedDoc, "color");
                    String processedCity = getElementTextContent(processedDoc, "city");
                    String processedOwner = getElementTextContent(processedDoc, "owner");

                    // Extract corresponding fields from original XML
                    String originalName = getElementTextContent(originalDoc, "name");
                    String originalTimestamp = getElementTextContent(originalDoc, "timestamp");
                    String originalValue = getElementTextContent(originalDoc, "value");
                    String originalType = getElementTextContent(originalDoc, "type");
                    String originalUnit = getElementTextContent(originalDoc, "unit");
                    String originalColor = getElementTextContent(originalDoc, "color");
                    String originalCity = getElementTextContent(originalDoc, "city");
                    String originalOwner = getElementTextContent(originalDoc, "owner");

                    // Verify transformation: city should be uppercase
                    assertThat(processedCity).isEqualTo(originalCity.toUpperCase())
                        .as("City should be uppercase: original='%s', processed='%s'", originalCity, processedCity);

                    log.info("Verified city transformation: '{}' -> '{}'", originalCity, processedCity);

                    // Verify other fields remain unchanged
                    assertThat(processedName).isEqualTo(originalName).as("Name should remain unchanged");
                    assertThat(processedTimestamp).isEqualTo(originalTimestamp).as("Timestamp should remain unchanged");
                    assertThat(processedValue).isEqualTo(originalValue).as("Value should remain unchanged");
                    assertThat(processedType).isEqualTo(originalType).as("Type should remain unchanged");
                    assertThat(processedUnit).isEqualTo(originalUnit).as("Unit should remain unchanged");
                    assertThat(processedColor).isEqualTo(originalColor).as("Color should remain unchanged");
                    assertThat(processedOwner).isEqualTo(originalOwner).as("Owner should remain unchanged");

                } catch (Exception e) {
                    throw new AssertionError("Failed to parse and compare XML documents. Original: " + originalXmlValue + ", Processed: " + processedXmlValue, e);
                }
            });
        }

        // Note: Log checking is not available when running KSMLRunner directly in-process
        // The transformation validation is done through consuming the output topics above

        log.info("XML data format processing test completed successfully!");
        log.info("KSML generated XML sensor data using producer-xml.yaml");
        log.info("KSML processed XML data and transformed cities to uppercase using processor-xml.yaml");
        log.info("XML transformation and processing working correctly");
    }

    private void waitForSensorDataGeneration() throws Exception {
        log.info("Waiting for sensor data generation to start...");

        // Producer generates every 3 seconds, so wait for at least 2 messages
        // Use AdminClient to check actual message count instead of fixed sleep
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "ksml_sensordata_xml",
            2, // Wait for at least 2 messages
            Duration.ofSeconds(30) // Maximum 30 seconds (much better than fixed 7s)
        );

        log.info("Sensor data has been generated and verified");
    }

    private Properties createConsumerProperties(String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    /**
     * Parse an XML string into a DOM Document using standard Java XML parser.
     * This validates that the XML is well-formed and provides structured access to elements.
     *
     * @param xmlString The XML string to parse
     * @return Document object representing the parsed XML
     */
    private Document parseXmlDocument(String xmlString) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xmlString)));
    }

    /**
     * Extract the text content of an XML element by tag name.
     *
     * @param doc The XML document
     * @param tagName The tag name to extract
     * @return The text content of the element
     */
    private String getElementTextContent(Document doc, String tagName) {
        return doc.getElementsByTagName(tagName).item(0).getTextContent();
    }

}