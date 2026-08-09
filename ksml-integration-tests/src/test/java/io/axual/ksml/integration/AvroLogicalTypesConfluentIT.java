package io.axual.ksml.integration;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.integration.testutil.SharedKsmlInfra;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the Avro logical types tutorial end to end through the <b>Confluent</b> Avro serdes.
 *
 * <p>{@link AvroLogicalTypesIT} covers the same feature through the Apicurio serdes. The two paths do not
 * share a serializer: Apicurio writes its own wire header and resolves schemas through
 * {@code /apis/registry/v3}, while the Confluent serdes write the 4-byte Confluent header and resolve
 * through {@code /apis/ccompat/v7}. A logical type is carried by the schema, so a fault in either
 * serializer's schema handling would show up on one path only. This test closes that gap.</p>
 *
 * <p>It also keeps the tutorial honest. The definitions it runs are exactly the files that
 * {@code docs/tutorials/beginner/avro-logical-types.md} includes, so a change that breaks the tutorial
 * breaks this test.</p>
 *
 * <p>The decimal is the case that matters most. It crosses the wire as raw bytes, reaches Python as the
 * text {@code "123.45"}, gets {@code 0.05} added with Python's {@code Decimal}, and must come back as
 * exactly {@code "123.50"}. A float would not survive that trip intact.</p>
 */
@Slf4j
@Testcontainers
class AvroLogicalTypesConfluentIT {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String OUTPUT_TOPIC = "logical_types_json";

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/beginner-tutorial/avro-logical-types",
                    "producer-logical-types.yaml", "processor-logical-types.yaml", "SensorReading.avsc")
            .withKafka(SharedKsmlInfra.kafka())
            .withConfluentAvroRegistry(SharedKsmlInfra.schemaRegistry())
            .withTopics("logical_types_avro", OUTPUT_TOPIC)
            .dependsOn(SharedKsmlInfra.kafka(), SharedKsmlInfra.schemaRegistry());

    @Test
    @DisplayName("Logical types survive the Confluent Avro round trip and the decimal stays exact")
    void logicalTypesSurviveConfluentAvroRoundTrip() throws Exception {
        KSMLRunnerTestUtil.waitForTopicMessages(
                SharedKsmlInfra.kafka().getBootstrapServers(), OUTPUT_TOPIC, 2, Duration.ofSeconds(60));

        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        final var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedKsmlInfra.kafka().getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-logical-types-confluent");

        try (final var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
            final var records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(15));

            assertThat(records).as("Should have converted records in " + OUTPUT_TOPIC).isNotEmpty();
            log.info("Found {} converted logical-type messages", records.count());

            final var softly = new SoftAssertions();
            records.forEach(consumerRecord -> {
                log.info("JSON logical value: key={}, value={}", consumerRecord.key(), consumerRecord.value());
                try {
                    final var json = JSON.readTree(consumerRecord.value());
                    softly.assertThat(consumerRecord.key()).as("key").startsWith("sensor");

                    softly.assertThat(json.path("value").isString())
                            .as("decimal reaches Python and JSON as a string, not as a number").isTrue();
                    softly.assertThat(json.path("value").asString())
                            .as("123.45 plus 0.05 stays exact through the Confluent serdes").isEqualTo("123.50");

                    softly.assertThat(json.path("readingId").asString())
                            .as("uuid value").isEqualTo("123e4567-e89b-12d3-a456-426614174000");
                    softly.assertThatCode(() -> UUID.fromString(json.path("readingId").asString()))
                            .as("uuid is well formed").doesNotThrowAnyException();

                    softly.assertThat(json.path("sensor").asString())
                            .as("a plain string field is untouched").startsWith("sensor");
                    softly.assertThat(json.path("measuredOn").asInt())
                            .as("date, days since 1970-01-01").isEqualTo(19785);
                    softly.assertThat(json.path("measuredAt").asLong())
                            .as("timestamp-millis").isEqualTo(1700000000123L);
                } catch (Exception e) {
                    softly.fail("Could not parse JSON output: " + consumerRecord.value(), e);
                }
            });
            softly.assertAll();
        }

        log.info("Confluent Avro logical-types end-to-end test completed successfully");
    }
}
