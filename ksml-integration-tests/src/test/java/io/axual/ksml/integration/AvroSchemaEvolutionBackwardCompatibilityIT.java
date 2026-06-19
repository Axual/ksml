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
import io.apicurio.registry.serde.Legacy4ByteIdHandler;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
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
 * Integration tests for AVRO schema evolution compatibility scenarios.
 *
 * <p>These tests verify end-to-end behavior that unit tests cannot cover: actual Avro binary
 * serialization/deserialization with schema evolution via a running Apicurio registry. All
 * KSML-internal schema checks are already covered by AvroSchemaEvolutionCompatibilityTest.
 *
 * <p>Scenarios tested here:
 * <ol>
 *   <li>Backward compat — add optional field (matrix scenario 1): new schema reads old data.</li>
 *   <li>Backward compat — type promotion int→long (matrix scenario 5): Avro wire promotion.</li>
 * </ol>
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

        log.info("PHASE 1: Producing 3 messages with schema v1 (8 fields)");
        runPhase1_ProduceWithSchemaV1();

        log.info("PHASE 2: Processing v1 messages with schema v2 (10 fields)");
        runPhase2_ProcessWithSchemaV2();

        log.info("=".repeat(80));
        log.info("TEST PASSED: Schema evolution backward compatibility works!");
        log.info("=".repeat(80));
    }

    private void runPhase1_ProduceWithSchemaV1() {
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

            log.info("Waiting for 3 messages to be produced with schema v1...");
            Awaitility.await("Wait for 3 messages in sensor_data_evolution_test")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_evolution_test");
                        log.debug("Found {} messages in topic sensor_data_evolution_test", count);
                        return count >= 3;
                    });

            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase1-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_evolution_test"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have produced exactly 3 messages").isEqualTo(3);
                log.info("Phase 1 complete: Produced {} messages with schema v1 (8 fields)", records.count());

                // Log sample keys
                records.forEach(consumerRecord -> log.debug("  Phase 1 message: key={}", consumerRecord.key()));
            }
        } finally {
            ksmlPhase1.stop();
            log.info("Phase 1 KSML stopped");
        }
    }

    private void runPhase2_ProcessWithSchemaV2() {
        final KSMLContainer ksmlPhase2 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro-schema-evolution-tests-phase2",
                        "phase2-processor.yaml", "SensorData.avsc")
                .withKafka(kafka)
                .withApicurioAvroRegistry(schemaRegistry)
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase2.start();
            log.info("Phase 2 KSML started successfully");

            log.info("Waiting for v1 messages to be processed with v2 schema...");
            Awaitility.await("Wait for 3 messages in sensor_data_evolution_processed")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_evolution_processed");
                        log.debug("Found {} messages in topic sensor_data_evolution_processed", count);
                        return count >= 3;
                    });

            final String registryUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081) + "/apis/registry/v2";
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-phase2-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
            consumerProps.put("apicurio.registry.url", registryUrl);
            // Match KSML's apicurio-avro serde configuration (Confluent-compatible 4-byte contentId).
            // See ApicurioAvroSerdeSupplier.ApicurioAvroSerde.modifyConfigs.
            consumerProps.put(SerdeConfig.ENABLE_HEADERS, "false");
            consumerProps.put(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER, "true");
            consumerProps.put(SerdeConfig.USE_ID, "contentId");
            consumerProps.put(SerdeConfig.ID_HANDLER, Legacy4ByteIdHandler.class.getName());

            try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_evolution_processed"));
                ConsumerRecords<String, Object> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have processed all 3 messages").isEqualTo(3);
                log.info("Phase 2 complete: Processed {} messages with schema v2 (10 fields)", records.count());

                // Verify all messages were processed successfully
                records.forEach(consumerRecord -> {
                    log.debug("  Phase 2 processed message: key={}", consumerRecord.key());
                    assertThat(consumerRecord.key()).as("Key should match sensor pattern")
                            .matches("sensor[0-9]");

                    GenericRecord value = (GenericRecord) consumerRecord.value();
                    assertThat(value.get("location")).as("location field should be null (v2 default)").isNull();
                    assertThat(value.get("accuracy")).as("accuracy field should be null (v2 default)").isNull();
                });
            }

            log.info("SUCCESS: All v1 messages (8 fields) processed successfully with v2 schema (10 fields)");
            log.info("Backward compatibility verified: Optional field addition works correctly");
        } finally {
            ksmlPhase2.stop();
            log.info("Phase 2 KSML stopped");
        }
    }

    @Test
    void testBackwardCompatibility_TypePromotion_IntToLong() {
        log.info("=".repeat(80));
        log.info("AVRO SCHEMA EVOLUTION TYPE PROMOTION TEST (int -> long)");
        log.info("=".repeat(80));

        log.info("PHASE 1: Producing 3messages with int reading field");
        runTypePromoPhase1_ProduceWithIntSchema();

        log.info("PHASE 2: Processing int-schema messages with long reading field schema");
        runTypePromoPhase2_ProcessWithLongSchema();

        log.info("=".repeat(80));
        log.info("TEST PASSED: Type promotion int->long works correctly!");
        log.info("=".repeat(80));
    }

    private void runTypePromoPhase1_ProduceWithIntSchema() {
        final KSMLContainer ksmlPhase1 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro-schema-evolution-type-promo-tests-phase1",
                        "phase1-type-promo-producer.yaml", "SensorData.avsc")
                .withKafka(kafka)
                .withApicurioAvroRegistry(schemaRegistry)
                .withTopics("sensor_data_avro_type_promo_test", "sensor_data_avro_type_promo_processed")
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase1.start();
            log.info("Type-promo Phase 1 KSML started successfully");

            log.info("Waiting for 3 messages to be produced with int reading...");
            Awaitility.await("Wait for 3 messages in sensor_data_avro_type_promo_test")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_avro_type_promo_test");
                        log.debug("Found {} messages in topic sensor_data_avro_type_promo_test", count);
                        return count >= 3;
                    });

            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-type-promo-phase1-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_avro_type_promo_test"));
                ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have produced exactly 3 messages").isEqualTo(3);
                log.info("Type-promo Phase 1 complete: Produced {} messages with int reading", records.count());

                records.forEach(entry -> log.debug("  Type-promo Phase 1 message: key={}", entry.key()));
            }
        } finally {
            ksmlPhase1.stop();
            log.info("Type-promo Phase 1 KSML stopped");
        }
    }

    private void runTypePromoPhase2_ProcessWithLongSchema() {
        final KSMLContainer ksmlPhase2 = new KSMLContainer()
                .withKsmlFiles("/docs-examples/beginner-tutorial/different-data-formats/avro-schema-evolution-type-promo-tests-phase2",
                        "phase2-type-promo-processor.yaml", "SensorData.avsc")
                .withKafka(kafka)
                .withApicurioAvroRegistry(schemaRegistry)
                .dependsOn(kafka, schemaRegistry);

        try {
            ksmlPhase2.start();
            log.info("Type-promo Phase 2 KSML started successfully");

            log.info("Waiting for int-schema messages to be processed with long schema...");
            Awaitility.await("Wait for 3 messages in sensor_data_avro_type_promo_processed")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        long count = getTopicMessageCount(kafka.getBootstrapServers(), "sensor_data_avro_type_promo_processed");
                        log.debug("Found {} messages in topic sensor_data_avro_type_promo_processed", count);
                        return count >= 3;
                    });

            final String registryUrl = "http://localhost:" + schemaRegistry.getMappedPort(8081) + "/apis/registry/v2";
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-type-promo-phase2-verifier");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
            consumerProps.put("apicurio.registry.url", registryUrl);
            // Match KSML's apicurio-avro serde configuration (Confluent-compatible 4-byte contentId).
            // See ApicurioAvroSerdeSupplier.ApicurioAvroSerde.modifyConfigs.
            consumerProps.put(SerdeConfig.ENABLE_HEADERS, "false");
            consumerProps.put(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER, "true");
            consumerProps.put(SerdeConfig.USE_ID, "contentId");
            consumerProps.put(SerdeConfig.ID_HANDLER, Legacy4ByteIdHandler.class.getName());

            try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList("sensor_data_avro_type_promo_processed"));
                ConsumerRecords<String, Object> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

                assertThat(records.count()).as("Should have processed exactly 3 messages").isEqualTo(3);
                log.info("Type-promo Phase 2 complete: Processed {} messages with long reading schema", records.count());

                records.forEach(entry -> {
                    log.debug("  Type-promo Phase 2 processed message: key={}", entry.key());
                    assertThat(entry.key()).as("Key should match sensor pattern")
                            .matches("sensor[0-9]");

                    GenericRecord value = (GenericRecord) entry.value();
                    Object readingObj = value.get("reading");
                    assertThat(readingObj).as("reading field should be a Long after type promotion").isInstanceOf(Long.class);
                    int sensorIndex = Integer.parseInt(entry.key().substring("sensor".length()));
                    long expectedReading = sensorIndex + 1L;
                    assertThat((Long) readingObj)
                            .as("int reading must survive promotion to long unchanged")
                            .isEqualTo(expectedReading);
                });
            }

            log.info("SUCCESS: All int-reading messages processed successfully with long-reading schema");
            log.info("Type promotion int->long backward compatibility verified");
        } finally {
            ksmlPhase2.stop();
            log.info("Type-promo Phase 2 KSML stopped");
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
