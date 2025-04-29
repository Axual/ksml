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

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.graalvm.home.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.metric.Metrics;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Named.named;

@Slf4j
@EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
class KafkaProducerRunnerTest {
    public static final int MAXIMUM_WAIT_TIME = 90; // Number of seconds to wait for the producer to finish

    MockProducer<byte[], byte[]> mockProducer;

    @BeforeEach
    void cleanProducer() {
        // Clean all metrics
        Metrics.registry().removeAll();
        // Create a new mockProducer for testing
        mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @ParameterizedTest
    @DisplayName("Verify producer stop conditions and pattern support")
    @MethodSource
    void verifyProducer(String topologyDefinition, Map<String, String> config, Map<String, String> expectedProducerConfig, int expectedRecordsProduced) throws Exception {
        // given a topology with a single shot produce and a runner for it
        var topologyDefinitionMap = loadDefinitions(topologyDefinition);
        var testConfig = new KafkaProducerRunner.Config(topologyDefinitionMap, config);
        var mockProducerSupplier = new MockProducerSupplier(mockProducer);
        var producerRunner = new KafkaProducerRunner(testConfig, mockProducerSupplier);

        final var thread = new Thread(producerRunner);
        thread.start();
        // when the runner starts in a separate thread and runs for some time
        Awaitility.await("Wait for the producer to finish")
                .atMost(Duration.ofSeconds(MAXIMUM_WAIT_TIME))
                .until(() -> !thread.isAlive());

        // Stop producer runner explicitly
        producerRunner.stop();


        // then when the runner has executed, only one record is produced.
        log.info("history size={}", mockProducer.history().size());
        final var softly = new SoftAssertions();
        softly.assertThat(mockProducer.history())
                .as("Check the number of produced records")
                .isNotNull()
                .size().isEqualTo(expectedRecordsProduced);

        softly.assertThat(mockProducerSupplier.capturedCreateProducerArguments)
                .as("Check the captured producer configuration is invoked only once with the expected configuration")
                .size().isOne()
                .returnToIterable()
                .first()
                .isEqualTo(expectedProducerConfig);

        softly.assertAll();
    }

    static Stream<Arguments> verifyProducer() {
        final var inputConfigWithoutPatterns = Map.of(
                "AnotherKey", "value"
        );
        final var expectedConfigWithoutPatterns = Map.of(
                "AnotherKey", "value",
                "key.serializer", ByteArraySerializer.class,
                "value.serializer", ByteArraySerializer.class
        );
        final var inputConfigWithCompatPatterns = Map.of(
                ResolvingClientConfig.COMPAT_TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}",
                ResolvingClientConfig.COMPAT_GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}",
                ResolvingClientConfig.COMPAT_TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}",
                "AnotherKey", "value"
        );
        final var inputConfigWithCurrentPatterns = Map.of(
                ResolvingClientConfig.TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}",
                ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}",
                ResolvingClientConfig.TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}",
                "AnotherKey", "value"
        );
        final var expectedConfigWithCurrentPattern = Map.of(
                ResolvingClientConfig.TOPIC_PATTERN_CONFIG, "{AnotherKey}-{topic}",
                ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG, "{AnotherKey}-{group.id}",
                ResolvingClientConfig.TRANSACTIONAL_ID_PATTERN_CONFIG, "{AnotherKey}-{transactional.id}",
                "AnotherKey", "value",
                "key.serializer", ByteArraySerializer.class,
                "value.serializer", ByteArraySerializer.class
        );

        return Stream.of(
                Arguments.of(named("NO PATTERN - Producing can stop based on a condition", "produce-test-condition.yaml"), inputConfigWithoutPatterns, expectedConfigWithoutPatterns, 2),
                Arguments.of(named("COMPAT PATTERN - Producing can stop based on a condition", "produce-test-condition.yaml"), inputConfigWithCompatPatterns, expectedConfigWithCurrentPattern, 2),
                Arguments.of(named("CURRENT PATTERN - Producing can stop based on a condition", "produce-test-condition.yaml"), inputConfigWithCurrentPatterns, expectedConfigWithCurrentPattern, 2),
                Arguments.of(named("NO PATTERN - A fixed count of records can be produced", "produce-test-count-3.yaml"), inputConfigWithoutPatterns, expectedConfigWithoutPatterns, 3),
                Arguments.of(named("COMPAT PATTERN - A fixed count of records can be produced", "produce-test-count-3.yaml"), inputConfigWithCompatPatterns, expectedConfigWithCurrentPattern, 3),
                Arguments.of(named("CURRENT PATTERN - A fixed count of records can be produced", "produce-test-count-3.yaml"), inputConfigWithCurrentPatterns, expectedConfigWithCurrentPattern, 3),
                Arguments.of(named("NO PATTERN - when `interval` is omitted only 1 record is produced", "produce-test-single.yaml"), inputConfigWithoutPatterns, expectedConfigWithoutPatterns, 1),
                Arguments.of(named("COMPAT PATTERN - when `interval` is omitted only 1 record is produced", "produce-test-single.yaml"), inputConfigWithCompatPatterns, expectedConfigWithCurrentPattern, 1),
                Arguments.of(named("CURRENT PATTERN - when `interval` is omitted only 1 record is produced", "produce-test-single.yaml"), inputConfigWithCurrentPatterns, expectedConfigWithCurrentPattern, 1)
        );
    }

    /**
     * Load a topology definition from the given file in test/resources
     *
     * @param filename ksml definition file
     * @return a Map containing the parsed definition
     * @throws IOException        if loading fails
     * @throws URISyntaxException for invalid file name
     */
    private Map<String, TopologyDefinition> loadDefinitions(String filename) throws IOException, URISyntaxException {
        final var mapper = new NativeDataObjectMapper();
        final var jsonNotation = new JsonNotation(mapper);
        NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(mapper, jsonNotation::serde), null);
        NotationLibrary.register(JsonNotation.NOTATION_NAME, jsonNotation, new JsonDataObjectConverter());

        final var uri = ClassLoader.getSystemResource(filename).toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        return ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
    }

    /**
     * Supplier for mock producer
     */
    static class MockProducerSupplier implements Function<Map<String, Object>, Producer<byte[], byte[]>> {
        final Producer<byte[], byte[]> mockProducer;
        final List<Map<String, Object>> capturedCreateProducerArguments = new ArrayList<>();

        /**
         * Instantiate the A with the given configuration and the mockProducer to return
         *
         * @param mockProducer the mocked producer to return
         */
        public MockProducerSupplier(Producer<byte[], byte[]> mockProducer) {
            this.mockProducer = mockProducer;
        }

        @Override
        public Producer<byte[], byte[]> apply(final Map<String, Object> producerConfig) {
            capturedCreateProducerArguments.add(Collections.unmodifiableMap(producerConfig));
            return mockProducer;
        }
    }

    static boolean isRunningOnGraalVM() {
        return Version.getCurrent().isRelease();
    }
}