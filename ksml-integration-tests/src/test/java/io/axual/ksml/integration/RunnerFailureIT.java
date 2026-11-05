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
import io.axual.ksml.integration.testutil.SharedKsmlInfra;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the KSML runner process exits with a non-zero exit code when Kafka Streams fails, so
 * Docker or Kubernetes can restart it.
 *
 * <p>The failing record is produced by the test after KSML has started, so a correct runner does
 * not exit while {@link KSMLContainer} is still waiting for it to start.</p>
 */
@Slf4j
@Testcontainers
class RunnerFailureIT {

    private static final String INPUT_TOPIC = "crash_in";
    private static final String OUTPUT_TOPIC = "crash_out";

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/runner-shutdown/failure", "ratio-processor.yaml")
            .withKafka(SharedKsmlInfra.kafka())
            .withTopics(INPUT_TOPIC, OUTPUT_TOPIC)
            .dependsOn(SharedKsmlInfra.kafka());

    @Test
    void processExitsWithNonZeroCodeWhenKafkaStreamsFails() throws Exception {
        final Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedKsmlInfra.kafka().getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(config)) {
            // Dividing by zero raises a ZeroDivisionError in the pipeline, which moves Kafka Streams to ERROR
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "k2", "{\"amount\": 10, \"div\": 0}"));
            producer.flush();
        }

        final var exitCode = ksml.waitForExit(Duration.ofSeconds(60));

        assertThat(exitCode)
                .as("KSML should exit after Kafka Streams failed, so it can be restarted")
                .isPresent();
        // run() returns 1; any other code means a crash or an outside kill
        assertThat(exitCode.get()).as("exit code").isEqualTo(1);
    }
}
