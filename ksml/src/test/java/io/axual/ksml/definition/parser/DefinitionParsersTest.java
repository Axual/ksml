package io.axual.ksml.definition.parser;

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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.AutoOffsetReset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefinitionParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void registerNotations() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
    }

    private static ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    // --- OffsetResetPolicyParser -----------------------------------------------------------------

    @ParameterizedTest
    @DisplayName("a null or empty reset policy string is parsed as null")
    @NullAndEmptySource
    void parsesNullOrEmptyResetPolicyAsNull(String input) {
        assertThat(OffsetResetPolicyParser.parseResetPolicy(input)).isNull();
    }

    @Test
    @DisplayName("named reset policies earliest, latest and none are parsed correctly")
    void parsesNamedResetPolicies() {
        assertThat(OffsetResetPolicyParser.parseResetPolicy("earliest")).isEqualTo(AutoOffsetReset.earliest());
        assertThat(OffsetResetPolicyParser.parseResetPolicy("latest")).isEqualTo(AutoOffsetReset.latest());
        assertThat(OffsetResetPolicyParser.parseResetPolicy("none")).isEqualTo(AutoOffsetReset.none());
    }

    @Test
    @DisplayName("a by_duration reset policy is parsed into a duration-based reset")
    void parsesByDurationResetPolicy() {
        assertThat(OffsetResetPolicyParser.parseResetPolicy("by_duration:10s"))
                .isEqualTo(AutoOffsetReset.byDuration(Duration.ofSeconds(10)));
    }

    @Test
    @DisplayName("an unknown reset policy string is rejected")
    void rejectsUnknownResetPolicy() {
        assertThatThrownBy(() -> OffsetResetPolicyParser.parseResetPolicy("nonsense"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown auto offset reset strategy");
    }

    // --- JoinTargetDefinitionParser --------------------------------------------------------------

    @Test
    @DisplayName("the join target parser returns null for a null node")
    void joinTargetReturnsNullForNullNode() {
        assertThat(new JoinTargetDefinitionParser(resources).parse(null)).isNull();
    }

    @Test
    @DisplayName("the join target parser parses an inline stream definition")
    void joinTargetParsesInlineStream() throws Exception {
        final var target = new JoinTargetDefinitionParser(resources)
                .parse(nodeOf("stream:\n  topic: other\n  keyType: string\n  valueType: string"));
        assertThat(target).isNotNull();
        assertThat(target.definition().topic()).isEqualTo("other");
    }

    @Test
    @DisplayName("the join target parser resolves a table reference by name")
    void joinTargetParsesTableReference() throws Exception {
        resources.register("someTable",
                new TableDefinition("some_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        final var target = new JoinTargetDefinitionParser(resources).parse(nodeOf("table: someTable"));
        assertThat(target).isNotNull();
        assertThat(target.definition()).isInstanceOf(TableDefinition.class);
        assertThat(target.definition().topic()).isEqualTo("some_table");
    }

    @Test
    @DisplayName("the join target parser resolves a global table reference by name")
    void joinTargetParsesGlobalTableReference() throws Exception {
        resources.register("someGlobalTable",
                new GlobalTableDefinition("some_global_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        final var target = new JoinTargetDefinitionParser(resources).parse(nodeOf("globalTable: someGlobalTable"));
        assertThat(target).isNotNull();
        assertThat(target.definition()).isInstanceOf(GlobalTableDefinition.class);
        assertThat(target.definition().topic()).isEqualTo("some_global_table");
    }

    @Test
    @DisplayName("the join target parser fails when no valid target is present")
    void joinTargetFailsWhenNoTargetPresent() throws Exception {
        final var node = nodeOf("foo: bar");
        final var parser = new JoinTargetDefinitionParser(resources);
        assertThatThrownBy(() -> parser.parse(node)).isInstanceOf(ParseException.class);
    }

    // --- Table / GlobalTable definition parsers --------------------------------------------------

    @Test
    @DisplayName("a table definition is parsed from YAML")
    void parsesTableDefinition() throws Exception {
        final var table = new TableDefinitionParser(resources, false).parser()
                .parse(nodeOf("topic: my_table\nkeyType: string\nvalueType: string"));
        assertThat(table).isInstanceOf(TableDefinition.class);
        assertThat(((TableDefinition) table).topic()).isEqualTo("my_table");
    }

    @Test
    @DisplayName("a global table definition is parsed from YAML")
    void parsesGlobalTableDefinition() throws Exception {
        final var globalTable = new GlobalTableDefinitionParser(resources, false).parser()
                .parse(nodeOf("topic: my_global_table\nkeyType: string\nvalueType: string"));
        assertThat(globalTable).isInstanceOf(GlobalTableDefinition.class);
        assertThat(((GlobalTableDefinition) globalTable).topic()).isEqualTo("my_global_table");
    }
}
