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
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
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

import static org.assertj.core.api.Assertions.assertThat;
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

        final var uri = ClassLoader.getSystemResource("pipelines/" + nr + "-demo.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        final var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        log.info("{}", description);

        URI referenceURI = ClassLoader.getSystemResource("reference/" + nr + "-reference.txt").toURI();
        // Get the reference and clean the newlines
        String reference = cleanDescription(Files.readString(Paths.get(referenceURI)));

        assertThat(cleanDescription(description.toString())).isEqualTo(reference);
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("cogroup with a stream that a later pipeline saves fails the topology build")
    void cogroupStreamSavedByLaterPipelineIsRejected() throws Exception {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-wrong-order.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("grouped_refunds")
                .hasMessageContaining("not found")
                .hasMessageContaining("defined before this one");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("a pipeline that continues from a cogrouped stream that another pipeline added a stream to fails the topology build")
    void cogroupedStreamExtendedByOtherPipelineIsRejected() throws Exception {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-extended-twice.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("with_third")
                .hasMessageContaining("already has a stream added by another pipeline");
    }

    @Test
    @EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
    @DisplayName("aggregating a saved window of a cogroup that another pipeline added a stream to fails the topology build")
    void windowedCogroupOfExtendedCogroupIsRejected() throws Exception {
        assertThatThrownBy(() -> createTopology("pipelines/test-cogroup-windowed-extended.yaml"))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("windowed_totals")
                .hasMessageContaining("already has a stream added by another pipeline");
    }

    private void createTopology(String resource) throws Exception {
        final var uri = ClassLoader.getSystemResource(resource).toURI();
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(Paths.get(uri)), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        new TopologyGenerator("some.app.id").create(streamsBuilder, definitions);
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
