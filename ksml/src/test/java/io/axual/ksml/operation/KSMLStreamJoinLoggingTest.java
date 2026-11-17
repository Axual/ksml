package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.axual.ksml.testutil.LogCapture.stderrOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Streams always logs the window stores of a stream-stream join that the application provides, which KSML always
 * does. The {@code logging} of the join stores controls the shared store of a left or outer join: on unless both
 * stores set {@code logging: false}.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLStreamJoinLoggingTest {
    // Closed after each test, to free the Python contexts of the topologies
    private final List<TopologyGenerator> generators = new ArrayList<>();

    @AfterEach
    void closeGenerators() {
        generators.forEach(TopologyGenerator::close);
    }

    @KSMLTopic(topic = "input1")
    TestInputTopic<String, String> input1;

    @KSMLTopic(topic = "input2")
    TestInputTopic<String, String> input2;

    @KSMLDriver
    TopologyTestDriver topologyTestDriver;

    // Records without a match, so all join stores, including the outer join's shared store, get a write
    private void pipeUnmatchedRecords() {
        input1.pipeInput("a", "left", 1000L);
        input2.pipeInput("b", "right", 1000L);
    }

    private long changelogTopicCount() {
        return topologyTestDriver.producedTopicNames().stream().filter(topic -> topic.endsWith("-changelog")).count();
    }

    @KSMLTest(topology = "pipelines/test-stream-join-logging-default.yaml")
    @DisplayName("an outer join whose stores leave out logging logs its window stores and its shared store")
    void joinStoresLogByDefault() {
        pipeUnmatchedRecords();

        assertThat(changelogTopicCount()).isEqualTo(3);
        assertThat(topologyTestDriver.producedTopicNames())
                .anyMatch(topic -> topic.contains("outer-shared-join") && topic.endsWith("-changelog"));
    }

    @Test
    @DisplayName("with logging: false on both stores, only the shared store has no changelog, and each store gets a warning")
    void joinStoresWithoutLogging() throws Exception {
        final var topology = new AtomicReference<Topology>();
        final var log = stderrOf(() -> topology.set(buildTopology("pipelines/test-stream-join-logging-off.yaml")));

        assertThat(log).contains(warning("this_store")).contains(warning("other_store")).doesNotContain("shared store");
        assertThat(changelogsAfterUnmatchedRecords(topology.get())).hasSize(2)
                .anyMatch(topic -> topic.endsWith("-this_store-changelog"))
                .anyMatch(topic -> topic.endsWith("-other_store-changelog"))
                .noneMatch(topic -> topic.contains("outer-shared-join"));
    }

    @Test
    @DisplayName("with logging: false on one store, the shared store keeps its changelog, and that store gets a warning")
    void mixedLoggingKeepsSharedChangelog() throws Exception {
        final var topology = new AtomicReference<Topology>();
        final var log = stderrOf(() -> topology.set(buildTopology("pipelines/test-stream-join-logging-mixed.yaml")));

        assertThat(log).contains(warning("other_store")).doesNotContain("this_store").doesNotContain("shared store");
        assertThat(changelogsAfterUnmatchedRecords(topology.get())).hasSize(3)
                .anyMatch(topic -> topic.contains("outer-shared-join"));
    }

    @Test
    @DisplayName("a left join whose stores leave out logging logs its window stores and its shared store")
    void leftJoinStoresLogByDefault() throws Exception {
        final var topology = buildTopology("pipelines/test-stream-join-logging-left-default.yaml");

        assertThat(changelogsAfterUnmatchedRecords(topology)).hasSize(3)
                .anyMatch(topic -> topic.contains("left-shared-join"));
    }

    @Test
    @DisplayName("a left join with logging: false on both stores has no changelog only for its shared store")
    void leftJoinStoresWithoutLogging() throws Exception {
        final var topology = new AtomicReference<Topology>();
        final var log = stderrOf(() -> topology.set(buildTopology("pipelines/test-stream-join-logging-left-off.yaml")));

        assertThat(log).contains(warning("this_store")).contains(warning("other_store"));
        assertThat(changelogsAfterUnmatchedRecords(topology.get())).hasSize(2)
                .noneMatch(topic -> topic.contains("shared-join"));
    }

    @Test
    @DisplayName("an inner join with logging: false on both stores warns for each store, and its window stores still log")
    void innerJoinStoresWithoutLogging() throws Exception {
        final var topology = new AtomicReference<Topology>();
        final var log = stderrOf(() -> topology.set(buildTopology("pipelines/test-stream-join-logging-inner-off.yaml")));

        assertThat(log).contains(warning("this_store")).contains(warning("other_store"));
        assertThat(changelogsAfterUnmatchedRecords(topology.get())).hasSize(2)
                .anyMatch(topic -> topic.endsWith("-this_store-changelog"))
                .anyMatch(topic -> topic.endsWith("-other_store-changelog"));
    }

    // Pipes a record without a match into each input, so every join store gets a write, and returns the changelogs
    private static List<String> changelogsAfterUnmatchedRecords(Topology topology) {
        try (var driver = new TopologyTestDriver(topology)) {
            driver.createInputTopic("input1", new StringSerializer(), new StringSerializer()).pipeInput("a", "left", 1000L);
            driver.createInputTopic("input2", new StringSerializer(), new StringSerializer()).pipeInput("b", "right", 1000L);
            return driver.producedTopicNames().stream().filter(topic -> topic.endsWith("-changelog")).toList();
        }
    }

    private Topology buildTopology(String resource) throws Exception {
        final var path = Paths.get(ClassLoader.getSystemResource(resource).toURI());
        final var yaml = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definition = new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(yaml, "join_logging"));
        final var generator = new TopologyGenerator("join_logging.app", null, PythonContextConfig.builder().build());
        generators.add(generator);
        return generator.create(new StreamsBuilder(), Map.of("definition", definition));
    }

    /** The warning for a join store with logging: false, true for inner, left and outer joins alike. */
    private static String warning(String store) {
        return "Join store '" + store + "' sets 'logging: false', but Kafka Streams always logs join stores";
    }
}
