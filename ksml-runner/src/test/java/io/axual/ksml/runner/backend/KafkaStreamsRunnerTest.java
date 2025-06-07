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

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

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

    @Disabled("Somehow Kafka Streams mock gets stuck during these tests, needs further investigation")
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

        var runner = new KafkaStreamsRunner(config, mockStreamsSupplier);

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

            verify(mockStreams, Mockito.timeout(60000).times(1)).start();
            assertThat(future)
                    .as("Future should not have finished yet")
                    .isNotDone();
            assertThat(runner.getState())
                    .as("Streams should be running until stopped")
                    .isEqualTo(Runner.State.STARTED);

            // Stop the runner
            runner.stop();


            Awaitility.await("Wait for runner to stop")
                    .atMost(Duration.ofSeconds(10))
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
