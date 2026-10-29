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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the KSML runner closes Kafka Streams when the process receives SIGTERM, as it does on
 * {@code docker stop} or a Kubernetes pod stop, so the offsets of processed records are committed.
 *
 * <p>The runner configuration sets a commit interval far longer than the test, so the only commit
 * that can store the offsets is the one Kafka Streams does when it is closed.</p>
 */
@Slf4j
@Testcontainers
class RunnerShutdownIT {

    private static final String APPLICATION_ID = "io.ksml.runner.shutdown.sigterm.test";
    private static final String INPUT_TOPIC = "stop_in";
    private static final String OUTPUT_TOPIC = "stop_count_out";
    private static final int RECORD_COUNT = 10;

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/runner-shutdown/sigterm", "count-processor.yaml")
            .withKafka(SharedKsmlInfra.kafka())
            .withTopics(INPUT_TOPIC, OUTPUT_TOPIC)
            .dependsOn(SharedKsmlInfra.kafka());

    @Test
    void sigtermCommitsOffsetsOfProcessedRecords() throws Exception {
        final var bootstrapServers = SharedKsmlInfra.kafka().getBootstrapServers();
        produceRecords(bootstrapServers);

        // Every record updates the count, so the output topic gets one record per input record
        KSMLRunnerTestUtil.waitForTopicMessages(bootstrapServers, OUTPUT_TOPIC, RECORD_COUNT, Duration.ofSeconds(60));

        final var exitCode = ksml.terminate(Duration.ofSeconds(60));
        assertThat(exitCode).as("KSML should exit after SIGTERM").isPresent();
        log.info("KSML exited with code {} after SIGTERM", exitCode.get());

        assertThat(committedOffset(bootstrapServers))
                .as("Kafka Streams should commit the offsets of all processed records when it is closed on SIGTERM")
                .isNotNull()
                .extracting(OffsetAndMetadata::offset)
                .isEqualTo((long) RECORD_COUNT);
    }

    private static void produceRecords(String bootstrapServers) {
        final Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(config)) {
            for (int i = 1; i <= RECORD_COUNT; i++) {
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "a", "{\"n\":" + i + "}"));
            }
            producer.flush();
        }
    }

    private static OffsetAndMetadata committedOffset(String bootstrapServers) throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            final var offsets = admin.listConsumerGroupOffsets(APPLICATION_ID)
                    .partitionsToOffsetAndMetadata()
                    .get();
            log.info("Committed offsets for {}: {}", APPLICATION_ID, offsets);
            return offsets.get(new TopicPartition(INPUT_TOPIC, 0));
        }
    }
}
