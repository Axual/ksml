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
import io.axual.ksml.runner.producer.ExecutableProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.axual.ksml.runner.exception.RunnerException;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    @DisplayName("A failing producer factory moves the runner to FAILED and reports a RunnerException")
    void failingProducerFactoryFailsTheRunner() {
        final Function<Map<String, Object>, Producer<byte[], byte[]>> failingFactory = config -> {
            throw new IllegalStateException("cannot create producer");
        };
        final var runner = new KafkaProducerRunner(configWith(Map.of()), failingFactory);

        assertThatThrownBy(runner::run)
                .isInstanceOf(RunnerException.class)
                .hasMessageContaining("Unhandled producer exception")
                .hasCauseInstanceOf(IllegalStateException.class);

        assertThat(runner.getState()).isEqualTo(Runner.State.FAILED);
    }

    @Test
    @DisplayName("The public constructor wires the default resolving producer and runs with no producers")
    void publicConstructorUsesDefaultProducerFactory() {
        // Exercises the production constructor (default ResolvingProducer factory). With no producer
        // definitions the real producer is created from the config but nothing is ever scheduled.
        final var config = KafkaProducerRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(Map.of("bootstrap.servers", "localhost:9092"))
                .pythonContextConfig(PythonContextConfig.builder().build())
                .build();
        final var runner = new KafkaProducerRunner(config);

        runner.run();

        assertThat(runner.getState()).isEqualTo(Runner.State.STOPPED);
    }

    @Test
    @DisplayName("runScheduledProducers produces, reschedules while requested, and stops when nothing is due")
    void runScheduledProducersReschedulesUntilDone() {
        final var runner = new KafkaProducerRunner(configWith(Map.of()), new CapturingProducerFactory());
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        // A stubbed producer that asks to be rescheduled once, then stops.
        final var executableProducer = mock(ExecutableProducer.class);
        when(executableProducer.shouldReschedule()).thenReturn(true, false);
        when(executableProducer.interval()).thenReturn(Duration.ZERO);
        runner.scheduler.schedule(executableProducer);

        runner.runScheduledProducers(mockProducer);

        // Produced on the initial run and once more after the single reschedule.
        verify(executableProducer, times(2)).produceMessages(mockProducer);
        assertThat(runner.scheduler.hasScheduledItems()).isFalse();
    }

    @Test
    @DisplayName("runScheduledProducers does not reschedule a producer that is finished")
    void runScheduledProducersStopsWhenNotRescheduling() {
        final var runner = new KafkaProducerRunner(configWith(Map.of()), new CapturingProducerFactory());
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        final var executableProducer = mock(ExecutableProducer.class);
        when(executableProducer.shouldReschedule()).thenReturn(false);
        runner.scheduler.schedule(executableProducer);

        runner.runScheduledProducers(mockProducer);

        verify(executableProducer, times(1)).produceMessages(mockProducer);
        assertThat(runner.scheduler.hasScheduledItems()).isFalse();
    }
}
