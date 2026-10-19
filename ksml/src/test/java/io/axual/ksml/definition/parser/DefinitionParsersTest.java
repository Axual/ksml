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

import tools.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.JoinWithTableOperation;
import io.axual.ksml.operation.parser.JoinOperationParser;
import io.axual.ksml.parser.MapParser;
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

    // --- Names saved with "as" -------------------------------------------------------------------

    private static final String JOIN_WITH_SAVED_CUSTOMERS = """
            name: joinOp
            type: join
            table: saved_customers
            valueJoiner:
              expression: value1
              resultType: string
            """;

    // Parses the pipelines like TopologyDefinitionParser does, so their saved names are registered in the resources
    private Object parsePipelines(String yaml) throws Exception {
        resources.register("orders", new StreamDefinition("orders", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null));
        resources.register("customers", new StreamDefinition("customers", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null));
        return new MapParser<>("pipeline", "pipeline definition", new PipelineDefinitionParser(resources)).parse(nodeOf(yaml));
    }

    private BaseOperation parseJoinWithSavedCustomers() throws Exception {
        return new JoinOperationParser(resources).parser().parse(nodeOf(JOIN_WITH_SAVED_CUSTOMERS));
    }

    @Test
    @DisplayName("a join can use a table that a parsed pipeline saved with \"as\"")
    void joinWithTableSavedWithAsParses() throws Exception {
        parsePipelines("""
                save_pipeline:
                  from: customers
                  via:
                    - type: toTable
                  as: saved_customers
                """);
        assertThat(parseJoinWithSavedCustomers()).isInstanceOf(JoinWithTableOperation.class);
    }

    @Test
    @DisplayName("a join can use the result of a parsed pipeline without a sink, by the pipeline name")
    void joinWithResultOfPipelineWithoutSinkParses() throws Exception {
        parsePipelines("""
                saved_customers:
                  from: customers
                  via:
                    - type: toTable
                """);
        assertThat(parseJoinWithSavedCustomers()).isInstanceOf(JoinWithTableOperation.class);
    }

    @Test
    @DisplayName("a join can not use a name before the pipeline that saves it is parsed, so the saving pipeline must come first")
    void joinBeforeSavingPipelineIsRejected() throws Exception {
        assertThatThrownBy(this::parseJoinWithSavedCustomers)
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown table \"saved_customers\". Define it under \"tables\", or save a table with \"as: saved_customers\" in a pipeline defined before this one");

        parsePipelines("""
                save_pipeline:
                  from: customers
                  via:
                    - type: toTable
                  as: saved_customers
                """);
        assertThat(parseJoinWithSavedCustomers()).isInstanceOf(JoinWithTableOperation.class);
    }

    @Test
    @DisplayName("an \"as\" name that is defined under tables is rejected")
    void asNameDefinedAsTableIsRejected() throws Exception {
        resources.register("orders_table", new TableDefinition("orders_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        final var yaml = """
                latest_orders:
                  from: orders
                  via:
                    - type: toTable
                  as: orders_table
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"latest_orders\"")
                .hasStackTraceContaining("\"as: orders_table\" is already defined under \"tables\". Use another name");
    }

    @Test
    @DisplayName("an \"as\" name that an earlier pipeline already saves is rejected")
    void duplicateAsNameIsRejected() throws Exception {
        final var yaml = """
                first:
                  from: orders
                  via:
                    - type: toTable
                  as: latest_orders
                second:
                  from: customers
                  via:
                    - type: toTable
                  as: latest_orders
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"second\"")
                .hasStackTraceContaining("\"as: latest_orders\" is already used by pipeline \"first\". Use another name");
    }

    @Test
    @DisplayName("a pipeline without a sink, named like a name that an earlier pipeline already saves, is rejected")
    void pipelineNameSavedByEarlierPipelineIsRejected() throws Exception {
        final var yaml = """
                first:
                  from: orders
                  via:
                    - type: toTable
                  as: latest_orders
                latest_orders:
                  from: customers
                  via:
                    - type: toTable
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"latest_orders\"")
                .hasStackTraceContaining("Without a sink, pipeline \"latest_orders\" saves its result under \"latest_orders\", which is already used by pipeline \"first\". Add a sink, or rename the pipeline");
    }

    @Test
    @DisplayName("an \"as\" name that a branch of an earlier pipeline already saves is rejected, naming the pipeline and branch")
    void asNameSavedByEarlierBranchIsRejected() throws Exception {
        final var yaml = """
                splitter:
                  from: orders
                  branch:
                    - if:
                        expression: value is not None
                      as: dup
                    - to: customers
                later:
                  from: customers
                  as: dup
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"later\"")
                .hasStackTraceContaining("\"as: dup\" is already used by pipeline \"splitter\" (branch1). Use another name");
    }

    @Test
    @DisplayName("two branches of one pipeline with the same \"as\" name are rejected")
    void asNameSavedByTwoBranchesIsRejected() throws Exception {
        final var yaml = """
                splitter:
                  from: orders
                  branch:
                    - if:
                        expression: value is not None
                      as: dup
                    - as: dup
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"splitter\"")
                // FatalError drops the cause below the branch entry, so only check which branch fails
                .hasStackTraceContaining("Error in branch entry 2");
    }

    @Test
    @DisplayName("an \"as\" name in a branch that an earlier pipeline already saves is rejected")
    void asNameInBranchSavedByEarlierPipelineIsRejected() throws Exception {
        final var yaml = """
                first:
                  from: customers
                  as: dup
                splitter:
                  from: orders
                  branch:
                    - if:
                        expression: value is not None
                      as: dup
                    - to: customers
                """;
        assertThatThrownBy(() -> parsePipelines(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Error in pipeline definition \"splitter\"")
                // FatalError drops the cause below the branch entry, so only check which branch fails
                .hasStackTraceContaining("Error in branch entry 1");
    }

    @Test
    @DisplayName("the place where a name is saved is described by the pipeline and its branches")
    void describesWhereANameIsSaved() {
        assertThat(TopologyResources.pipelineOf(new MetricTags().append("pipeline", "orders")))
                .isEqualTo("pipeline \"orders\"");
        assertThat(TopologyResources.pipelineOf(new MetricTags().append("pipeline", "splitter").append("branch", "branch2").append("branch", "branch1")))
                .isEqualTo("pipeline \"splitter\" (branch2, branch1)");
        assertThat(TopologyResources.pipelineOf(new MetricTags()))
                .isEqualTo("an earlier pipeline");
    }
}
