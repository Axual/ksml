package io.axual.ksml.definition.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.notation.MockNotation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Parses every definition in the repo, so a field that a parser reads but leaves out of its schema
 * fails a test instead of stopping KSML at startup.
 */
class AllDefinitionsParseTest {
    private static final List<String> SCHEMA_NOTATIONS = List.of("avro", "confluent_avro", "apicurio_avro", "protobuf",
            "apicurio_protobuf", "jsonschema", "confluent_jsonschema", "apicurio_jsonschema", "csv", "xml", "soap");

    @BeforeAll
    static void registerNotations() {
        final var context = new NotationContext(new DataObjectFlattener(), new DataTypeFlattener());
        final var json = new JsonNotation(context);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(context, json::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, json);
        // Field names do not depend on schemas, so a stand-in without schema files is enough
        SCHEMA_NOTATIONS.forEach(name -> ExecutionContext.INSTANCE.notationLibrary()
                .register(name, new MockNotation(name, Notation.SchemaUsage.SCHEMA_OPTIONAL, ".x", (_, _, _) -> null)));
    }

    static Stream<Path> definitions() throws IOException, URISyntaxException {
        final var root = projectRoot();
        final var result = Stream.<Path>builder();
        for (final var dir : List.of("ksml/src/test/resources", "examples", "ksml-integration-tests/src/test/resources/docs-examples")) {
            try (var files = Files.walk(root.resolve(dir))) {
                files.filter(path -> path.toString().endsWith(".yaml"))
                        // Runner configs, not definitions
                        .filter(path -> !path.getFileName().toString().startsWith("ksml-"))
                        // Build output of the examples module
                        .filter(path -> !path.startsWith(root.resolve("examples/target")))
                        // Unknown fields on purpose
                        .filter(path -> !path.toString().contains("unknown-keys") && !path.toString().contains("parity/usability"))
                        .forEach(result);
            }
        }
        return result.build().sorted();
    }

    // ksml/target/test-classes -> ksml/target -> ksml -> project root, so it works from any working directory
    private static Path projectRoot() throws URISyntaxException {
        final var testClasses = AllDefinitionsParseTest.class.getResource("/");
        if (testClasses == null) {
            throw new IllegalStateException("Test classes directory not found");
        }
        return Paths.get(testClasses.toURI()).getParent().getParent().getParent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("definitions")
    void definitionHasNoUnknownField(Path file) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(Files.readString(file), JsonNode.class);
        dropSchemaNames(root);
        final var thrown = catchThrowable(() -> new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(root, "test")));
        // Some test definitions fail on purpose, but never with an unknown field
        for (var cause = thrown; cause != null; cause = cause.getCause()) {
            assertThat(cause.getMessage()).doesNotContain("Unknown field");
        }
    }

    // "avro:SensorData" becomes "avro"
    private static void dropSchemaNames(JsonNode node) {
        if (node instanceof ObjectNode object) {
            for (final var name : List.copyOf(object.propertyNames())) {
                final var child = object.get(name);
                final var text = child.isString() ? child.asString() : "";
                final var colon = text.indexOf(':');
                if (colon > 0 && SCHEMA_NOTATIONS.contains(text.substring(0, colon))) object.put(name, text.substring(0, colon));
                else dropSchemaNames(child);
            }
        } else if (node.isArray()) {
            node.forEach(AllDefinitionsParseTest::dropSchemaNames);
        }
    }
}
