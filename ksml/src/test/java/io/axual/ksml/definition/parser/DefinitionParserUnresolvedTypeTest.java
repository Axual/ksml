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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.UnresolvedType;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.notation.MockNotation;
import io.axual.ksml.operation.parser.ConvertValueOperationParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the {@code allowUnresolved} flags that the definition parsers pass to
 * {@code userTypeField(...)}.
 * <p>
 * A type written without a schema name (for example {@code remote_avro}) becomes an
 * {@link UnresolvedType}: the schema is fetched from the registry at runtime. This is allowed for
 * topic-backed types and state stores (streams, tables, topics, state stores). It is not allowed in
 * places that must be fully known at parse time:
 * <ul>
 *   <li>function result types and function parameter types</li>
 *   <li>the target type of a convert operation</li>
 * </ul>
 * These tests parse through the real parsers, so a future change that flips a flag back is caught.
 */
class DefinitionParserUnresolvedTypeTest {
    // The error UserTypeParser returns when an unresolved type is used where it is not allowed.
    private static final String UNRESOLVED_NOT_ALLOWED = "Unspecified schema can only be used in the context of a topic";

    // A notation that has no inline schema but can fetch one from a remote registry.
    // "remote_avro" with no schema name therefore resolves to an UnresolvedType.
    private static final String REMOTE_NOTATION = "remote_avro";

    @BeforeAll
    static void registerNotations() {
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);

        final var remoteNotation = new MockNotation(REMOTE_NOTATION, Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", null) {
            @Override
            public boolean supportsRemoteSchema() {
                return true;
            }

            @Override
            public DataSchema fetchRemoteSchema(String topic, boolean isKey) {
                return new StructSchema(null, topic, null, Collections.emptyList());
            }
        };
        ExecutionContext.INSTANCE.notationLibrary().register(REMOTE_NOTATION, remoteNotation);
    }

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUpSchemaDirectory() {
        ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(tempDir.toString());
    }

    @AfterEach
    void clearSchemaDirectory() {
        ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory("");
    }

    private ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    // --- allowUnresolved = true: topic-backed types and state stores may omit the schema name ---

    @Test
    @DisplayName("Stream valueType may be a registry-backed notation without a schema name")
    void streamValueTypeWithoutSchemaNameParses() throws Exception {
        final var parser = new StreamDefinitionParser(new TopologyBaseResources("test"), false).parser();
        final var node = nodeOf("topic: some_topic\nkeyType: string\nvalueType: " + REMOTE_NOTATION);

        final var stream = assertDoesNotThrow(() -> parser.parse(node));
        assertNotNull(stream);
    }

    @Test
    @DisplayName("KeyValue state store valueType may be a registry-backed notation without a schema name")
    void stateStoreValueTypeWithoutSchemaNameParses() throws Exception {
        final var parser = new KeyValueStateStoreDefinitionParser(false, true).parser();
        final var node = nodeOf("name: my_store\nkeyType: string\nvalueType: " + REMOTE_NOTATION);

        final var store = assertDoesNotThrow(() -> parser.parse(node));
        assertNotNull(store);
        assertInstanceOf(UnresolvedType.class, store.valueType().dataType());
    }

    // --- allowUnresolved = false: the type must be fully known at parse time ---

    @Test
    @DisplayName("Function resultType without a schema name is rejected with a clear error")
    void functionResultTypeWithoutSchemaNameIsRejected() throws Exception {
        final var parser = new GenericFunctionDefinitionParser(false).parser();
        final var node = nodeOf("resultType: " + REMOTE_NOTATION + "\ncode: \"x = 1\"");

        final var ex = assertThrows(ParseException.class, () -> parser.parse(node));
        assertThat(ex.getMessage(), containsString(UNRESOLVED_NOT_ALLOWED));
    }

    @Test
    @DisplayName("Function parameter type without a schema name is rejected with a clear error")
    void parameterTypeWithoutSchemaNameIsRejected() throws Exception {
        final var parser = new ParameterDefinitionParser().parser();
        final var node = nodeOf("name: my_param\ntype: " + REMOTE_NOTATION);

        final var ex = assertThrows(ParseException.class, () -> parser.parse(node));
        assertThat(ex.getMessage(), containsString(UNRESOLVED_NOT_ALLOWED));
    }

    @Test
    @DisplayName("Convert 'into' type without a schema name is rejected with a clear error")
    void convertIntoTypeWithoutSchemaNameIsRejected() throws Exception {
        final var parser = new ConvertValueOperationParser(new TopologyResources("test")).parser();
        final var node = nodeOf("into: " + REMOTE_NOTATION);

        final var ex = assertThrows(ParseException.class, () -> parser.parse(node));
        assertThat(ex.getMessage(), containsString(UNRESOLVED_NOT_ALLOWED));
    }
}
