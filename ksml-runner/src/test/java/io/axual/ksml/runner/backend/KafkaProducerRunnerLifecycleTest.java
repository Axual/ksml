package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lifecycle tests for {@link KafkaProducerRunner} that do not require a Python/GraalVM runtime: with no
 * producer definitions the runner exercises its full state machine and producer configuration without
 * ever needing to build an {@code ExecutableProducer}. The message-producing paths (which do need
 * GraalVM) are covered by {@code KafkaProducerRunnerTest}.
 */
class KafkaProducerRunnerLifecycleTest {

    /** Captures the configuration the runner passes to the producer factory. */
    private static class CapturingProducerFactory implements Function<Map<String, Object>, Producer<byte[], byte[]>> {
        final List<Map<String, Object>> captured = new ArrayList<>();

        @Override
        public Producer<byte[], byte[]> apply(Map<String, Object> producerConfig) {
            captured.add(producerConfig);
            return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }
    }

    private static KafkaProducerRunner.Config configWith(Map<String, String> kafkaConfig) {
        return KafkaProducerRunner.Config.builder()
                .definitions(Map.of()) // no producers -> no Python needed
                .kafkaConfig(kafkaConfig)
                .pythonContextConfig(PythonContextConfig.builder().build())
                .build();
    }

    @Test
    @DisplayName("A runner with no producer definitions starts and stops cleanly")
    void runsToCompletionWithoutProducers() {
        final var runner = new KafkaProducerRunner(configWith(Map.of("client.id", "test")), new CapturingProducerFactory());

        assertThat(runner.getState()).isEqualTo(Runner.State.CREATED);

        runner.run();

        // Nothing is scheduled, so the runner walks STARTING -> STARTED -> STOPPED and returns.
        assertThat(runner.getState()).isEqualTo(Runner.State.STOPPED);
    }

    @Test
    @DisplayName("The runner injects the byte-array serializers into the producer configuration")
    void injectsByteArraySerializers() {
        final var factory = new CapturingProducerFactory();
        final var runner = new KafkaProducerRunner(configWith(Map.of("bootstrap.servers", "localhost:9092")), factory);

        runner.run();

        assertThat(factory.captured).hasSize(1);
        final var producerConfig = factory.captured.get(0);
        assertThat(producerConfig)
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)
                // the user-supplied kafka configuration is preserved
                .containsEntry("bootstrap.servers", "localhost:9092");
    }

    @Test
    @DisplayName("stop() requested before running short-circuits the produce loop")
    void stopBeforeRunStillCompletes() {
        final var runner = new KafkaProducerRunner(configWith(Map.of()), new CapturingProducerFactory());

        runner.stop();
        runner.run();

        assertThat(runner.getState()).isEqualTo(Runner.State.STOPPED);
    }
}
