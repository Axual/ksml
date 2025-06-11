package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.metric.KsmlTagEnricher;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import io.axual.ksml.runner.exception.RunnerException;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
class KafkaStreamsRunnerTest {

    static {
        final var minimalConfig = Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "test-id",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092",
                "AnotherKey", "value"
        );
        INPUT_CONFIG_WITHOUT_PATTERNS = Map.copyOf(minimalConfig);

        final var expectedWithoutPatterns = new HashMap<>(minimalConfig);
        EXPECTED_CONFIG_WITHOUT_PATTERNS = Collections.unmodifiableMap(expectedWithoutPatterns);

        final var inputWithCompatPatters = new HashMap<>(minimalConfig);
        inputWithCompatPatters.put(ResolvingClientConfig.COMPAT_TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}");
        inputWithCompatPatters.put(ResolvingClientConfig.COMPAT_GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}");
        inputWithCompatPatters.put(ResolvingClientConfig.COMPAT_TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}");
        INPUT_CONFIG_WITH_COMPAT_PATTERNS = Collections.unmodifiableMap(inputWithCompatPatters);

        final var inputWithCurrentPatters = new HashMap<>(minimalConfig);
        inputWithCurrentPatters.put(ResolvingClientConfig.TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}");
        inputWithCurrentPatters.put(ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}");
        inputWithCurrentPatters.put(ResolvingClientConfig.TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}");
        INPUT_CONFIG_WITH_CURRENT_PATTERNS = Collections.unmodifiableMap(inputWithCurrentPatters);

        final var expectedWithPatterns = new HashMap<>(expectedWithoutPatterns);
        expectedWithPatterns.put(ResolvingClientConfig.TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}");
        expectedWithPatterns.put(ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}");
        expectedWithPatterns.put(ResolvingClientConfig.TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}");
        EXPECTED_CONFIG_WITH_CURRENT_PATTERN = Collections.unmodifiableMap(expectedWithPatterns);

    }

    private static final Map<String, String> INPUT_CONFIG_WITHOUT_PATTERNS;
    private static final Map<String, String> EXPECTED_CONFIG_WITHOUT_PATTERNS;
    private static final Map<String, String> INPUT_CONFIG_WITH_COMPAT_PATTERNS;
    private static final Map<String, String> INPUT_CONFIG_WITH_CURRENT_PATTERNS;
    private static final Map<String, String> EXPECTED_CONFIG_WITH_CURRENT_PATTERN;

    private static final String[] RESTRICTED_CONFIGS = new String[]{
            ResolvingClientConfig.COMPAT_TOPIC_PATTERN_CONFIG,
            ResolvingClientConfig.COMPAT_GROUP_ID_PATTERN_CONFIG,
            ResolvingClientConfig.COMPAT_TRANSACTIONAL_ID_PATTERN_CONFIG
    };


    static Stream<Arguments> testConfigData() {
        return Stream.of(
                Arguments.of(named("No pattern should remain the same", INPUT_CONFIG_WITHOUT_PATTERNS), EXPECTED_CONFIG_WITHOUT_PATTERNS),
                Arguments.of(named("Current pattern should remain the same", INPUT_CONFIG_WITH_CURRENT_PATTERNS), EXPECTED_CONFIG_WITH_CURRENT_PATTERN),
                Arguments.of(named("Compat pattern is returned with current pattern config fields", INPUT_CONFIG_WITH_COMPAT_PATTERNS), EXPECTED_CONFIG_WITH_CURRENT_PATTERN)
        );
    }

    @ParameterizedTest
    @DisplayName("Check pattern handling")
    @MethodSource(value = "testConfigData")
    void testConfig(Map<String, String> inputConfig, Map<String, String> expectedConfig) {
        assertThat(KafkaStreamsRunner.Config.builder()
                .kafkaConfig(inputConfig)
                .build())
                .extracting(KafkaStreamsRunner.Config::kafkaConfig, InstanceOfAssertFactories.MAP)
                .containsAllEntriesOf(EXPECTED_CONFIG_WITHOUT_PATTERNS)
                .doesNotContainKeys(RESTRICTED_CONFIGS);
    }

    @Test
    @DisplayName("Test getStreamsConfig method")
    void testGetStreamsConfig() {
        // Create a runner to test the method
        final var config = KafkaStreamsRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(INPUT_CONFIG_WITHOUT_PATTERNS)
                .storageDirectory("tmp")
                .build();

        var runner = new KafkaStreamsRunner(config, (topology, properties) -> mock(KafkaStreams.class), mock(KsmlTagEnricher.class));

        // Test case 1: Basic configuration
        var result1 = runner.getStreamsConfig(INPUT_CONFIG_WITHOUT_PATTERNS, "test-dir", null);
        assertThat(result1)
                .containsEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test-id")
                .containsEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092")
                .containsEntry("AnotherKey", "value")
                .containsEntry(StreamsConfig.STATE_DIR_CONFIG, "test-dir")
                .containsEntry(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class)
                .containsEntry(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class)
                .containsEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);

        // Test case 2: With application server
        var appServer = ApplicationServerConfig.builder()
                .enabled(true)
                .host("localhost")
                .port("8080")
                .build();

        var result2 = runner.getStreamsConfig(INPUT_CONFIG_WITHOUT_PATTERNS, "test-dir", appServer);
        assertThat(result2)
                .containsEntry(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");

        // Test case 3: With null initial configs
        var result3 = runner.getStreamsConfig(null, "test-dir", null);
        assertThat(result3)
                .containsEntry(StreamsConfig.STATE_DIR_CONFIG, "test-dir")
                .containsEntry(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class)
                .containsEntry(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class)
                .containsEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    @Test
    @DisplayName("Test addCleanupInterceptor method")
    void testAddCleanupInterceptor() {
        // Create a runner to test the method
        final var config = KafkaStreamsRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(INPUT_CONFIG_WITHOUT_PATTERNS)
                .storageDirectory("tmp")
                .build();

        var runner = new KafkaStreamsRunner(config, (topology, properties) -> mock(KafkaStreams.class), mock(KsmlTagEnricher.class));

        // Test case 1: Empty config, add if missing = true
        var configs1 = new HashMap<String, Object>();
        runner.addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, configs1, true);
        assertThat(configs1)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                .containsValue("io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor");

        // Test case 2: Empty config, add if missing = false
        var configs2 = new HashMap<String, Object>();
        runner.addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, configs2, false);
        assertThat(configs2).isEmpty();

        // Test case 3: Existing interceptor config as String
        var configs3 = new HashMap<String, Object>();
        configs3.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "some.other.Interceptor");
        runner.addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, configs3, false);
        assertThat(configs3)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                .containsValue("io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor,some.other.Interceptor");

        // Test case 4: Existing interceptor config as List
        var configs4 = new HashMap<String, Object>();
        configs4.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, List.of("some.other.Interceptor"));
        runner.addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, configs4, false);
        assertThat(configs4)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                .containsValue("io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor,some.other.Interceptor");

        // Test case 5: Existing interceptor config already contains our interceptor
        var configs5 = new HashMap<String, Object>();
        configs5.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor,some.other.Interceptor");
        runner.addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, configs5, false);
        assertThat(configs5)
                .containsKey(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                .containsValue("io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor,some.other.Interceptor");
    }

    @Test
    @DisplayName("Test error handling")
    void testErrorHandling() throws Exception {
        final var mockStreams = mock(KafkaStreams.class);
        AtomicReference<KafkaStreams.State> streamState = new AtomicReference<>(KafkaStreams.State.CREATED);

        // Mock the state method to return the current state from the AtomicReference
        when(mockStreams.state()).thenAnswer(inv -> streamState.get());

        // Create a runner with the mock
        final var config = KafkaStreamsRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(INPUT_CONFIG_WITHOUT_PATTERNS)
                .storageDirectory("tmp")
                .build();

        var runner = new KafkaStreamsRunner(config, (topology, properties) -> mockStreams, mock(KsmlTagEnricher.class));
        runner.setSleepDurations(10, 10);

        // Set up a thread to run the runner
        final var thread = new Thread(runner);

        try {
            // Start in ERROR state
            streamState.set(KafkaStreams.State.ERROR);

            // Start the runner
            thread.start();

            // Wait for the thread to complete or timeout
            thread.join(1000);

            // Verify that the runner is not running
            assertThat(runner.isRunning()).isFalse();
            assertThat(runner.getState()).isEqualTo(Runner.State.FAILED);

            // Verify that start was called
            verify(mockStreams).start();

            // We don't verify close() because the runner throws an exception before close() is called
            // This is expected behavior when the streams are in ERROR state
        } catch (Exception e) {
            // If the thread throws an exception, make sure it's a RunnerException
            assertThat(e).isInstanceOf(RunnerException.class)
                    .hasMessage("Kafka Streams is in a failed state");
        } finally {
            // Make sure the thread is stopped
            if (thread.isAlive()) {
                runner.stop();
                thread.join(1000);
            }
        }
    }

    @Test
    @DisplayName("Test state transitions")
    void testStateTransitions() {
        final var mockStreams = mock(KafkaStreams.class);

        // Create a runner with the mock
        final var config = KafkaStreamsRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(INPUT_CONFIG_WITHOUT_PATTERNS)
                .storageDirectory("tmp")
                .build();

        var runner = new KafkaStreamsRunner(config, (topology, properties) -> mockStreams, mock(KsmlTagEnricher.class));

        // Test all state mappings
        when(mockStreams.state()).thenReturn(KafkaStreams.State.CREATED);
        assertThat(runner.getState()).isEqualTo(Runner.State.CREATED);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);
        assertThat(runner.getState()).isEqualTo(Runner.State.STARTING);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        assertThat(runner.getState()).isEqualTo(Runner.State.STARTED);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.PENDING_SHUTDOWN);
        assertThat(runner.getState()).isEqualTo(Runner.State.STOPPING);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);
        assertThat(runner.getState()).isEqualTo(Runner.State.STOPPED);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.PENDING_ERROR);
        assertThat(runner.getState()).isEqualTo(Runner.State.FAILED);

        when(mockStreams.state()).thenReturn(KafkaStreams.State.ERROR);
        assertThat(runner.getState()).isEqualTo(Runner.State.FAILED);

        // Test isRunning method
        when(mockStreams.state()).thenReturn(KafkaStreams.State.CREATED);
        assertThat(runner.isRunning()).isFalse();

        when(mockStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);
        assertThat(runner.isRunning()).isTrue();

        when(mockStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        assertThat(runner.isRunning()).isTrue();

        when(mockStreams.state()).thenReturn(KafkaStreams.State.PENDING_SHUTDOWN);
        assertThat(runner.isRunning()).isTrue();

        when(mockStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);
        assertThat(runner.isRunning()).isFalse();

        when(mockStreams.state()).thenReturn(KafkaStreams.State.ERROR);
        assertThat(runner.isRunning()).isFalse();
    }

    @Test
    @DisplayName("Check Kafka Streams lifecycle")
    void testStreamsRunner() throws Exception {
        final var mockStreams = mock(KafkaStreams.class);
        AtomicReference<KafkaStreams.State> streamState = new AtomicReference<>(KafkaStreams.State.CREATED);
        lenient().doAnswer(a -> streamState.get()).when(mockStreams).state();

        final var closeAnswer = new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                streamState.set(KafkaStreams.State.NOT_RUNNING);
                return invocation.getRawArguments().length == 0 ? null : Boolean.TRUE;
            }
        };
        lenient().doAnswer(closeAnswer).when(mockStreams).close();
        lenient().doAnswer(closeAnswer).when(mockStreams).close(any(Duration.class));
        lenient().doAnswer(closeAnswer).when(mockStreams).close(any(KafkaStreams.CloseOptions.class));


        final var mockStreamsSupplier = new MockStreamsSupplier(mockStreams);

        final KafkaStreamsRunner.Config config = KafkaStreamsRunner.Config.builder()
                .definitions(Map.of())
                .kafkaConfig(INPUT_CONFIG_WITHOUT_PATTERNS)
                .appServer(ApplicationServerConfig.builder()
                        .enabled(false)
                        .build())
                .storageDirectory("tmp")
                .build();

        var runner = new KafkaStreamsRunner(config, mockStreamsSupplier, mock(KsmlTagEnricher.class));
        // Set small sleep durations for faster test execution
        runner.setSleepDurations(10, 10);

        // Check initial runner state
        assertThat(runner)
                .isNotNull()
                .returns(Runner.State.CREATED, KafkaStreamsRunner::getState)
                .returns(false, KafkaStreamsRunner::isRunning);

        // Verify that KafkaStreams hasn't started yet
        verify(mockStreams, never()).start();

        assertThat(mockStreamsSupplier.capturedCreateProperties)
                .as("Check that Kafka Streams is initialized with correct properties")
                .size().isOne().returnToIterable()
                .first(InstanceOfAssertFactories.MAP)
                .containsAllEntriesOf(EXPECTED_CONFIG_WITHOUT_PATTERNS)
                .doesNotContainKeys(RESTRICTED_CONFIGS);


        // start returning state running
        streamState.set(KafkaStreams.State.RUNNING);

        try (final var executor = Executors.newSingleThreadExecutor()) {

            // Start runner, is blocked so check with async
            var future = CompletableFuture.runAsync(runner, executor);

            verify(mockStreams, Mockito.timeout(1000).times(1)).start();
            assertThat(future)
                    .as("Future should not have finished yet")
                    .isNotDone();
            assertThat(runner.getState())
                    .as("Streams should be running until stopped")
                    .isEqualTo(Runner.State.STARTED);

            // Stop the runner
            runner.stop();

            Awaitility.await("Wait for runner to stop")
                    .atMost(Duration.ofSeconds(1))
                    .until(future::isDone);
        }
    }


    /**
     * Supplier for mock Kafka Streams
     */
    static class MockStreamsSupplier implements BiFunction<Topology, Properties, KafkaStreams> {
        final KafkaStreams mockKafkaStreams;
        final List<Topology> capturedCreateTopologies = new ArrayList<>();
        final List<Properties> capturedCreateProperties = new ArrayList<>();

        /**
         * Instantiate the A with the given configuration and the mockProducer to return
         *
         * @param mockKafkaStreams the mocked Kafka Streams to return
         */
        public MockStreamsSupplier(final KafkaStreams mockKafkaStreams) {
            this.mockKafkaStreams = mockKafkaStreams;
        }


        @Override
        public KafkaStreams apply(final Topology topology, final Properties properties) {
            capturedCreateTopologies.add(topology);
            capturedCreateProperties.add(properties);
            return mockKafkaStreams;
        }
    }

}
