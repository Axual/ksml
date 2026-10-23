package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import tools.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
class TopologyGeneratorBasicTest {
    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    @BeforeAll
    static void beforeAll() {
        log.debug("Registering test notations");
        final var jsonNotation = new JsonNotation();
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
    }

    @ParameterizedTest
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void parseAndCheckOuput(int nr) throws Exception {
        final var jsonNotation = new JsonNotation();
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, new BinaryNotation(jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        final var topology = createTopology("pipelines/" + nr + "-demo.yaml");
        final TopologyDescription description = topology.describe();
        log.info("{}", description);

        URI referenceURI = ClassLoader.getSystemResource("reference/" + nr + "-reference.txt").toURI();
        // Get the reference and clean the newlines
        String reference = cleanDescription(Files.readString(Paths.get(referenceURI)));

        assertThat(cleanDescription(description.toString())).isEqualTo(reference);
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("join with a table that a later pipeline saves fails parsing")
    void joinTableSavedByLaterPipelineIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-join-saved-table-wrong-order.yaml"))
                .hasMessageContaining("Error in pipeline definition \"join_pipeline\"")
                .rootCause()
                .hasMessageContaining("Unknown table \"t2joined\"");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("join with \"table\" naming a saved stream fails the topology build")
    void joinTableNamingSavedStreamIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-join-saved-stream-as-table.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Join table \"filtered_other\" must be a table, but found");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("join with \"globalTable\" naming a saved table fails parsing")
    void joinGlobalTableNamingSavedTableIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-join-saved-table-as-globaltable.yaml"))
                .hasMessageContaining("Error in pipeline definition \"join_pipeline\"")
                .rootCause()
                .hasMessageContaining("Unknown globalTable \"updates_table\"");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("saving a result with an \"as\" name that is defined under tables fails parsing")
    void asNameDefinedAsTableIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-as-name-clashes-with-table.yaml"))
                .hasMessageContaining("Error in pipeline definition \"latest_orders\"")
                .rootCause()
                .hasMessageContaining("\"as: orders\" is already defined under \"tables\"");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("a pipeline without sink named like a defined stream still builds")
    void pipelineNameDefinedAsStreamIsAccepted() {
        assertThatCode(() -> createTopology("pipelines/test-pipeline-name-clashes-with-stream.yaml"))
                .doesNotThrowAnyException();
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("a pipeline with a sink named like a defined stream builds, because it saves nothing under its name")
    void pipelineWithSinkNamedLikeStreamIsAccepted() {
        assertThatCode(() -> createTopology("pipelines/test-pipeline-with-sink-named-like-stream.yaml"))
                .doesNotThrowAnyException();
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("cogroup with a stream that a later pipeline saves fails the topology build")
    void cogroupStreamSavedByLaterPipelineIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-wrong-order.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("grouped_refunds")
                .hasMessageContaining("not found")
                .hasMessageContaining("defined before this one");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("a pipeline that continues from a cogrouped stream that another pipeline added a stream to fails the topology build")
    void cogroupedStreamExtendedByOtherPipelineIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-extended-twice.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("with_third")
                .hasMessageContaining("already has a stream added by another pipeline");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("aggregating a saved cogroup after another pipeline added a stream to it fails the topology build")
    void aggregateOfExtendedCogroupIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-extend-before-aggregate.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("only_first")
                .hasMessageContaining("already has a stream added by another pipeline");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("aggregating a saved window of a cogroup that another pipeline added a stream to fails the topology build")
    void windowedCogroupOfExtendedCogroupIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-windowed-extended.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("windowed_totals")
                .hasMessageContaining("already has a stream added by another pipeline");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("cogroup with a stream from \"streams:\" fails the topology build with a hint")
    void cogroupWithStreamsEntryIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-streams-entry.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Cogroup stream \"refunds\" is a topic definition");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("cogroup with a table from \"tables:\" fails the topology build with a hint")
    void cogroupWithTablesEntryIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-tables-entry.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Cogroup stream \"refunds\" is a topic definition");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("cogroup with a stream that another definition file saves fails the topology build")
    void cogroupStreamSavedInOtherFileIsRejected() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-other-file-saves.yaml", "pipelines/test-cogroup-other-file-uses.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("grouped_refunds")
                .hasMessageContaining("in the same definition file");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("without optimization, a grouped stream from groupBy in a cogroup and also in count fails the topology build with a hint")
    void repartitionedStreamInCogroupAndCountIsRejectedWithoutOptimization() {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-group-by-shared.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("can not be used in a cogroup and also in another operation when topology.optimization is not all");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("with optimization, a grouped stream from groupBy in a cogroup and also in count shares one repartition")
    void repartitionedStreamInCogroupAndCountWorksWithOptimization() throws Exception {
        final var topology = createTopologyWithOptimization(StreamsConfig.OPTIMIZE, "pipelines/test-cogroup-group-by-shared.yaml");
        final var props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        try (final var driver = new TopologyTestDriver(topology, props)) {
            final var first = driver.createInputTopic("first_topic", new StringSerializer(), new LongSerializer());
            final var second = driver.createInputTopic("second_topic", new StringSerializer(), new LongSerializer());
            final var output = driver.createOutputTopic("output_topic", new StringDeserializer(), new LongDeserializer());
            first.pipeInput("A", 1L);
            second.pipeInput("a", 2L);

            // The cogroup total, then the cogroup total and the count after the record from second_topic
            assertThat(output.readKeyValuesToList()).containsExactly(
                    new KeyValue<>("A", 1L),
                    new KeyValue<>("A", 21L),
                    new KeyValue<>("A", 1L));
        }
    }

    // Each resource is a separate definition file, built in the given order
    private Topology createTopology(String... resources) throws Exception {
        return createTopologyWithOptimization(null, resources);
    }

    private Topology createTopologyWithOptimization(String optimization, String... resources) throws Exception {
        final var definitions = ImmutableMap.<String, TopologyDefinition>builder();
        for (final var resource : resources) {
            final var uri = ClassLoader.getSystemResource(resource).toURI();
            final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(Paths.get(uri)), JsonNode.class);
            definitions.put(resource, new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        }
        return new TopologyGenerator("some.app.id", optimization, null).create(streamsBuilder, definitions.build());
    }

    /**
     * Evaluates the condition for the tests above.
     */
    static boolean isRunningOnGraalVM() {
        return Version.getCurrent().isRelease();
    }

    @ParameterizedTest
    @CsvSource({"foo,foo",
            "foo@2ee39e73,foo",
            "some text thing@2ee39e73 and some more,some text thing and some more",
            "some foo@2ee39e73 and some bar@3ab456dc also,some foo and some bar also",
            "leave short@123 alone,leave short@123 alone"
    })
    void cleanDescriptionTest(String input, String expected) {
        log.info("input='{}',expected='{}'", input, expected);
        assertThat(cleanDescription(input)).isEqualTo(expected);
    }

    @Test
    @DisplayName("cleanDescription also fixes line endings")
    void cleanDescriptionTestLineEnd() {
        assertThat(cleanDescription("fix\r\nnewlines")).isEqualTo("fix\nnewlines");
    }

    /**
     * Clean a description string by removing all object references ("@abcd1234") and fixing Windows line endings.
     */
    private String cleanDescription(String description) {
        return description
                .replaceAll("@[a-fA-F-0-9]{5,}", "")
                .replaceAll("\r\n", "\n")
                .trim();
    }
}
