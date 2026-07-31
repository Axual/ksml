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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.SharedKsmlInfra;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test that a record with Avro logical types (uuid, decimal, date, time-millis, timestamp-millis)
 * is produced through the real Apicurio Avro serializer, read back, and converted to JSON with every logical
 * value intact. The decimal is the key case: it crosses the wire as raw bytes and must come back as "123.45".
 */
@Slf4j
@Testcontainers
class AvroLogicalTypesIT {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/logical-types", "producer-logical.yaml", "processor-logical-convert.yaml", "LogicalData.avsc")
            .withKafka(SharedKsmlInfra.kafka())
            .withApicurioAvroRegistry(SharedKsmlInfra.schemaRegistry())
            .withTopics("logical_data_avro", "logical_data_json")
            .dependsOn(SharedKsmlInfra.kafka(), SharedKsmlInfra.schemaRegistry());

    @Test
    void logicalTypesSurviveAvroRoundTripAndConvertToJson() throws Exception {
        KSMLRunnerTestUtil.waitForTopicMessages(
                SharedKsmlInfra.kafka().getBootstrapServers(), "logical_data_json", 2, Duration.ofSeconds(60));

        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        final var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedKsmlInfra.kafka().getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-logical-json");

        try (final var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("logical_data_json"));
            final var records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(15));

            assertThat(records).as("Should have converted logical-type records in logical_data_json").isNotEmpty();
            log.info("Found {} converted logical-type messages", records.count());

            final var softly = new SoftAssertions();
            records.forEach(consumerRecord -> {
                log.info("JSON logical value: key={}, value={}", consumerRecord.key(), consumerRecord.value());
                try {
                    final var json = JSON.readTree(consumerRecord.value());
                    softly.assertThat(consumerRecord.key()).as("key").startsWith("sensor");
                    softly.assertThat(json.path("amount").isTextual()).as("amount is a JSON string").isTrue();
                    softly.assertThat(json.path("amount").asText()).as("decimal survived the avro wire").isEqualTo("123.45");
                    softly.assertThat(json.path("id").asText()).as("uuid value").isEqualTo("123e4567-e89b-12d3-a456-426614174000");
                    softly.assertThatCode(() -> UUID.fromString(json.path("id").asText())).as("uuid is well formed").doesNotThrowAnyException();
                    softly.assertThat(json.path("eventDate").asInt()).as("date").isEqualTo(19785);
                    softly.assertThat(json.path("eventTimeMillis").asInt()).as("time-millis").isEqualTo(3723000);
                    softly.assertThat(json.path("eventTimestamp").asLong()).as("timestamp-millis is positive").isPositive();
                } catch (Exception e) {
                    softly.fail("Could not parse JSON output: " + consumerRecord.value(), e);
                }
            });
            softly.assertAll();
        }

        log.info("Avro logical-types end-to-end test completed successfully");
    }
}
