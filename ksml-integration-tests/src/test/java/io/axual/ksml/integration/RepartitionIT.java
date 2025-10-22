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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML Integration Test for repartition operation and custom partitioning logic.
 * <p>
 * This test validates that KSML can:
 * 1. Use custom streamPartitioner function to control partition assignment
 * 2. Apply repartition operation with custom partitioner
 * 3. Properly distribute records to partitions based on custom logic
 * <p>
 * The repartition example demonstrates:
 * - Changing key from region to user_id (mapKey operation)
 * - Adding processing metadata (transformValue operation)
 * - Custom partitioning: even user numbers (002, 004) -> partitions 0-1, odd numbers (001, 003, 005) -> partitions 2-3
 * <p>
 * TopologyTestDriver cannot test this functionality because it doesn't expose partition assignments.
 * This integration test verifies the actual partitioning behavior by consuming records and checking their partition assignments.
 */
@Slf4j
@Testcontainers
class RepartitionIT {

    private final ObjectMapper objectMapper = new ObjectMapper();

    static final Network network = Network.newNetwork();

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/docs-examples/reference/operations",
                          "ksml-runner.yaml", "repartition-example-producer.yaml", "repartition-example-processor.yaml")
            .withKafka(kafka)
            .withTopics("user_activities", "repartitioned_activities")
            .withPartitions(4)  // 4 partitions as per example
            .dependsOn(kafka);

    @Test
    void testRepartitionWithCustomPartitioner() throws Exception {
        // Wait for activities to be generated and repartitioned
        log.info("Waiting for KSML to generate and repartition user activities...");
        waitForActivityGeneration();

        // Verify KSML is still running
        assertThat(ksml.isRunning()).as("KSML should still be running").isTrue();

        // Create consumer to read from repartitioned topic with partition info
        final Properties consumerProps = createConsumerProperties();

        Map<String, List<Integer>> userPartitions = new HashMap<>(); // Track which partitions each user goes to
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("repartitioned_activities"));
            ConsumerRecords<String, String> records = KSMLRunnerTestUtil.pollWithRetry(consumer, Duration.ofSeconds(10));

            assertThat(records).as("Should have repartitioned activities in repartitioned_activities topic").isNotEmpty();
            log.info("Found {} repartitioned messages across {} partitions", records.count(), records.partitions().size());

            // Collect all records with partition information
            records.forEach(record -> {
                allRecords.add(record);
                String userId = record.key();
                int partition = record.partition();

                userPartitions.computeIfAbsent(userId, k -> new ArrayList<>()).add(partition);

                log.info("Record: key={}, partition={}, value preview={}",
                         userId, partition, record.value().substring(0, Math.min(100, record.value().length())));
            });
        }

        // Validate we have records
        assertThat(allRecords).as("Should have collected repartitioned records").isNotEmpty();

        // Verify partitioning and transformations
        verifyPartitioningLogic(userPartitions);
        verifyKeyTransformationAndValueEnrichment(allRecords);
    }

    /**
     * Verifies that the custom partitioning logic correctly distributes users to partitions.
     * <p>
     * Validates:
     * - All records for the same user go to the same partition
     * - Even user numbers (002, 004) are routed to partitions 0-1
     * - Odd user numbers (001, 003, 005) are routed to partitions 2-3
     */
    private void verifyPartitioningLogic(Map<String, List<Integer>> userPartitions) {
        for (Map.Entry<String, List<Integer>> entry : userPartitions.entrySet()) {
            String userId = entry.getKey();
            List<Integer> partitions = entry.getValue();

            // Extract user number from user_001, user_002, etc.
            int userNum = Integer.parseInt(userId.split("_")[1]);

            // Verify all records for same user go to same partition
            assertThat(partitions.stream().distinct().count())
                .as("All records for user %s should go to same partition", userId)
                .isEqualTo(1);

            int actualPartition = partitions.getFirst();

            // Verify partitioning logic:
            // Even user numbers (002, 004) -> partitions 0-1
            // Odd user numbers (001, 003, 005) -> partitions 2-3
            if (userNum % 2 == 0) {
                // Even users
                assertThat(actualPartition)
                    .as("Even user %s (num %d) should be in partition 0 or 1", userId, userNum)
                    .isIn(0, 1);
            } else {
                // Odd users
                assertThat(actualPartition)
                    .as("Odd user %s (num %d) should be in partition 2 or 3", userId, userNum)
                    .isIn(2, 3);
            }

            log.info("✓ User {} (num {}) correctly routed to partition {} ({})",
                     userId, userNum, actualPartition, userNum % 2 == 0 ? "even" : "odd");
        }
    }

    /**
     * Verifies that key transformation and value enrichment operations were applied correctly.
     * <p>
     * Validates:
     * - Keys are transformed from region to user_id
     * - Values contain all required fields (user_id, activity_id, activity_type, region)
     * - Processing metadata is added (processing_info, original_region)
     * - Key matches the user_id in the value
     */
    private void verifyKeyTransformationAndValueEnrichment(List<ConsumerRecord<String, String>> allRecords) throws Exception {
        for (ConsumerRecord<String, String> record : allRecords) {
            String userId = record.key();

            // Key should be user_id (transformed from region)
            assertThat(userId).as("Key should be user_id").startsWith("user_");

            // Parse and validate value
            JsonNode value = objectMapper.readTree(record.value());

            // Validate required fields
            assertThat(value.has("user_id")).as("Should have user_id field").isTrue();
            assertThat(value.has("activity_id")).as("Should have activity_id field").isTrue();
            assertThat(value.has("activity_type")).as("Should have activity_type field").isTrue();
            assertThat(value.has("region")).as("Should have region field").isTrue();

            // Validate processing metadata was added by transformValue operation
            assertThat(value.has("processing_info")).as("Should have processing_info field added by pipeline").isTrue();
            assertThat(value.get("processing_info").asText())
                .as("Processing info should indicate repartitioning by user")
                .contains("Repartitioned by user:");

            assertThat(value.has("original_region")).as("Should have original_region field added by pipeline").isTrue();

            // Validate key matches user_id in value
            assertThat(value.get("user_id").asText()).as("Key should match user_id in value").isEqualTo(userId);
        }
    }

    private Properties createConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-repartitioned");
        return props;
    }

    private void waitForActivityGeneration() throws Exception {
        log.info("Waiting for activity generation to start...");

        // Producer generates every 3 seconds, wait for at least 5 messages to get good distribution
        KSMLRunnerTestUtil.waitForTopicMessages(
            kafka.getBootstrapServers(),
            "repartitioned_activities",
            5, // Wait for at least 5 messages for good test coverage
            Duration.ofSeconds(30)
        );

        log.info("Activities have been generated and repartitioned");
    }
}
