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

import io.axual.ksml.integration.testutil.ApicurioSchemaRegistryContainer;
import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that validates AVRO schema evolution backward compatibility fix.
 * <p>
 * This test reproduces the bug scenario that was fixed:
 * 1. Phase 1: Produce 10 messages with schema v1 (8 fields)
 * 2. Stop KSML
 * 3. Evolve schema to v2 (10 fields with optional location and accuracy)
 * 4. Phase 2: Process v1 messages with v2 schema
 *
 */
@Slf4j
@Testcontainers
class AvroSchemaEvolutionBackwardCompatibilityIT {

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

    @Test
    void testBackwardCompatibility_AddOptionalFields() {
        log.info("=".repeat(80));
        log.info("AVRO SCHEMA EVOLUTION BACKWARD COMPATIBILITY TEST");
        log.info("=".repeat(80));

        // Phase 1: Produce messages with schema v1 (8 fields)
        log.info("PHASE 1: Producing 10 messages with schema v1 (8 fields)");
        runPhase1_ProduceWithSchemaV1();

        // Phase 2: Process v1 messages with schema v2 (10 fields)
        log.info("PHASE 2: Processing v1 messages with schema v2 (10 fields)");
        runPhase2_ProcessWithSchemaV2();

        log.info("=".repeat(80));
        log.info("TEST PASSED: Schema evolution backward compatibility works!");
        log.info("=".repeat(80));
    }

    /**
     * Phase 1: Produce 10 messages with schema v1 (8 fields)
     */
    private void runPhase1_ProduceWithSchemaV1() {
        // Create KSML container with v1 schema
        final KSMLContainer ksmlPhase1 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro-schema-evolution-tests-phase1",
                        "phase1-producer.yaml", "SensorData.avsc")
                .withKafka(kafka)
                .withApicurioAvroRegistry(schemaRegistry)
                .withTopics("sensor_data_evolution_test", "sensor_data_evolution_processed")
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase1.start();
            log.info("Phase 1 KSML started successfully");

            // Wait for 10 messages to be produced using Awaitility
            log.info("Waiting for 10 messages to be produced with schema v1...");
            Awaitility.await("Wait for 10 messages in sensor_data_evolution_test")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_evolution_test");
                        log.debug("Found {} messages in topic sensor_data_evolution_test", count);
                        return count >= 10;
                    });

            // Verify we have exactly 10 messages
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase1-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_evolution_test"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have produced exactly 10 messages").isEqualTo(10);
                log.info("✓ Phase 1 complete: Produced {} messages with schema v1 (8 fields)", records.count());

                // Log sample keys
                records.forEach(record -> log.debug("  Phase 1 message: key={}", record.key()));
            }
        } finally {
            ksmlPhase1.stop();
            log.info("Phase 1 KSML stopped");
        }
    }

    /**
     * Phase 2: Process v1 messages (8 fields) with schema v2 (10 fields)
     * This tests backward compatibility: new consumer reading old data
     */
    private void runPhase2_ProcessWithSchemaV2() {
        // Create KSML container with v2 schema
        // Topics were already created in Phase 1, so we don't call withTopics() here
        final KSMLContainer ksmlPhase2 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro-schema-evolution-tests-phase2",
                        "phase2-processor.yaml", "SensorData.avsc")
                .withKafka(kafka)
                .withApicurioAvroRegistry(schemaRegistry)
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase2.start();
            log.info("Phase 2 KSML started successfully");

            // Wait for messages to be processed using Awaitility
            log.info("Waiting for v1 messages to be processed with v2 schema...");
            Awaitility.await("Wait for 10 messages in sensor_data_evolution_processed")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_evolution_processed");
                        log.debug("Found {} messages in topic sensor_data_evolution_processed", count);
                        return count >= 10;
                    });

            // Verify all 10 messages were processed
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase2-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_evolution_processed"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have processed all 10 messages").isEqualTo(10);
                log.info("Phase 2 complete: Processed {} messages with schema v2 (10 fields)", records.count());

                // Verify all messages were processed successfully
                records.forEach(record -> {
                    log.debug("  Phase 2 processed message: key={}", record.key());
                    assertThat(record.key()).as("Key should match sensor pattern")
                            .matches("sensor[0-9]");
                });
            }

            log.info("✓ SUCCESS: All v1 messages (8 fields) processed successfully with v2 schema (10 fields)");
            log.info("✓ Backward compatibility verified: Optional field addition works correctly");
        } finally {
            ksmlPhase2.stop();
            log.info("Phase 2 KSML stopped");
        }
    }

    /**
     * Gets the total message count in a topic by summing up the end offsets of all partitions.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param topicName        the topic to check
     * @return total message count across all partitions
     */
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
