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

import io.axual.ksml.integration.testutil.ApicurioSchemaRegistryContainer;
import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JSON Schema evolution compatibility scenarios.
 *
 * <p>These tests verify end-to-end behavior that unit tests cannot cover: actual JSON Schema
 * serialization/deserialization with schema evolution via a running Apicurio registry. All
 * KSML-internal schema checks are already covered by JsonSchemaEvolutionCompatibilityTest.
 *
 * <p>Scenarios tested here:
 * <ol>
 *   <li>Backward compat — add optional field (matrix scenario 1): new schema reads old data.</li>
 * </ol>
 */
@Slf4j
@Testcontainers
class JsonSchemaEvolutionIT {

    static final Network network = Network.newNetwork();

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static final ApicurioSchemaRegistryContainer schemaRegistry = new ApicurioSchemaRegistryContainer()
            .withNetwork(network);

    @Test
    void testBackwardCompatibility_AddOptionalFields_JsonSchema() {
        log.info("=".repeat(80));
        log.info("JSON SCHEMA EVOLUTION BACKWARD COMPATIBILITY TEST");
        log.info("=".repeat(80));

        // Phase 1: Produce messages with schema v1 (8 fields)
        log.info("PHASE 1: Producing 10 messages with schema v1 (8 fields)");
        runPhase1_ProduceWithSchemaV1();

        // Phase 2: Process v1 messages with schema v2 (10 fields)
        log.info("PHASE 2: Processing v1 messages with schema v2 (10 fields)");
        runPhase2_ProcessWithSchemaV2();

        log.info("=".repeat(80));
        log.info("TEST PASSED: JSON Schema evolution backward compatibility works!");
        log.info("=".repeat(80));
    }

    private void runPhase1_ProduceWithSchemaV1() {
        final KSMLContainer ksmlPhase1 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/json-schema-evolution-tests-phase1",
                        "phase1-json-producer.yaml", "SensorData.json")
                .withKafka(kafka)
                .withApicurioJsonRegistry(schemaRegistry)
                .withTopics("sensor_data_jsonschema_evolution", "sensor_data_jsonschema_evolution_processed")
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase1.start();
            log.info("Phase 1 KSML started successfully");

            log.info("Waiting for 10 messages to be produced with schema v1...");
            Awaitility.await("Wait for 10 messages in sensor_data_jsonschema_evolution")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_jsonschema_evolution");
                        log.debug("Found {} messages in topic sensor_data_jsonschema_evolution", count);
                        return count >= 10;
                    });

            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase1-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_jsonschema_evolution"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have produced exactly 10 messages").isEqualTo(10);
                log.info("Phase 1 complete: Produced {} messages with schema v1 (8 fields)", records.count());

                records.forEach(record -> log.debug("  Phase 1 message: key={}", record.key()));
            }
        } finally {
            ksmlPhase1.stop();
            log.info("Phase 1 KSML stopped");
        }
    }

    private void runPhase2_ProcessWithSchemaV2() {
        // Topics were already created in Phase 1, so withTopics() is not called here
        final KSMLContainer ksmlPhase2 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/json-schema-evolution-tests-phase2",
                        "phase2-json-processor.yaml", "SensorData.json")
                .withKafka(kafka)
                .withApicurioJsonRegistry(schemaRegistry)
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase2.start();
            log.info("Phase 2 KSML started successfully");

            log.info("Waiting for v1 messages to be processed with v2 schema...");
            Awaitility.await("Wait for 10 messages in sensor_data_jsonschema_evolution_processed")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_jsonschema_evolution_processed");
                        log.debug("Found {} messages in topic sensor_data_jsonschema_evolution_processed", count);
                        return count >= 10;
                    });

            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase2-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_jsonschema_evolution_processed"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have processed all 10 messages").isEqualTo(10);
                log.info("Phase 2 complete: Processed {} messages with schema v2 (10 fields)", records.count());

                records.forEach(record -> {
                    log.debug("  Phase 2 processed message: key={}", record.key());
                    assertThat(record.key()).as("Key should match sensor pattern")
                            .matches("sensor[0-9]");
                });
            }

            log.info("SUCCESS: All v1 messages (8 fields) processed successfully with v2 schema (10 fields)");
            log.info("Backward compatibility verified: Optional field addition works correctly");
        } finally {
            ksmlPhase2.stop();
            log.info("Phase 2 KSML stopped");
        }
    }

    private long getTopicMessageCount(String bootstrapServers, String topicName) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            var topicDescription = adminClient.describeTopics(Collections.singletonList(topicName))
                    .topicNameValues().get(topicName).get();

            Map<TopicPartition, OffsetSpec> partitionOffsetSpecs = new HashMap<>();
            for (var partition : topicDescription.partitions()) {
                partitionOffsetSpecs.put(
                        new TopicPartition(topicName, partition.partition()),
                        OffsetSpec.latest()
                );
            }

            ListOffsetsResult offsetsResult = adminClient.listOffsets(partitionOffsetSpecs);

            long totalMessages = 0;
            for (var entry : offsetsResult.all().get().entrySet()) {
                totalMessages += entry.getValue().offset();
            }
            return totalMessages;
        } catch (Exception e) {
            log.debug("Error checking topic messages: {}", e.getMessage());
            return 0;
        }
    }
}
