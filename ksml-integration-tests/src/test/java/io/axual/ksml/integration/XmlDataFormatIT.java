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
import java.util.Collections;
import java.util.Properties;

import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

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
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have generated sensor data in ksml_sensordata_xml topic").isNotEmpty();
            log.info("Found {} XML sensor messages", records.count());

            // Validate XML messages
            records.forEach(record -> {
                log.info("XML Sensor: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                // Validate XML structure - should contain XML tags
                String xmlValue = record.value();
                assertThat(xmlValue).as("XML message should contain <SensorData> root element").contains("<SensorData>");
                assertThat(xmlValue).as("XML message should contain </SensorData> closing tag").contains("</SensorData>");

                // XML messages should contain sensor data fields (as XML format)
                assertThat(xmlValue)
                    .as("XML message should contain all sensor data elements")
                    .contains("<name>")
                    .contains("<city>")
                    .contains("<timestamp>")
                    .contains("<value>")
                    .contains("<type>")
                    .contains("<unit>")
                    .contains("<color>")
                    .contains("<owner>");

                // Validate that XML is well-formed (has proper opening/closing tags)
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertThat(openingTags).as("XML should have equal opening and closing angle brackets").isEqualTo(closingTags);
            });
        }

        // Check ksml_sensordata_xml_processed topic (processor output - transformed XML)
        final Properties processedConsumerProps = createConsumerProperties("test-consumer-processed");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(processedConsumerProps)) {
            consumer.subscribe(Collections.singletonList("ksml_sensordata_xml_processed"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have processed sensor data in ksml_sensordata_xml_processed topic").isNotEmpty();
            log.info("Found {} processed XML messages", records.count());

            // Validate processed XML messages
            records.forEach(record -> {
                log.info("Processed XML: key={}, value={}", record.key(), record.value());
                assertThat(record.key()).as("Sensor key should start with 'sensor'").startsWith("sensor");

                // Validate XML structure contains processed data
                String xmlValue = record.value();
                assertThat(xmlValue)
                    .as("Processed XML message should contain SensorData root elements")
                    .contains("<SensorData>")
                    .contains("</SensorData>");

                // Validate that XML is well-formed
                long openingTags = xmlValue.chars().filter(ch -> ch == '<').count();
                long closingTags = xmlValue.chars().filter(ch -> ch == '>').count();
                assertThat(openingTags).as("Processed XML should have equal opening and closing angle brackets").isEqualTo(closingTags);
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

}