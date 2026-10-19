package io.axual.ksml.operation.parser;

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

import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.JoinWithGlobalTableOperation;
import io.axual.ksml.operation.JoinWithStreamOperation;
import io.axual.ksml.operation.JoinWithTableOperation;
import io.axual.ksml.operation.LeftJoinWithGlobalTableOperation;
import io.axual.ksml.operation.LeftJoinWithStreamOperation;
import io.axual.ksml.operation.LeftJoinWithTableOperation;
import io.axual.ksml.operation.OuterJoinWithStreamOperation;
import io.axual.ksml.operation.OuterJoinWithTableOperation;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static io.axual.ksml.operation.parser.OperationParserTestSupport.nodeOf;
import static io.axual.ksml.operation.parser.OperationParserTestSupport.registerNotations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JoinOperationParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void setUp() {
        registerNotations();
        resources.register("theTable",
                new TableDefinition("the_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        resources.register("theGlobalTable",
                new GlobalTableDefinition("the_global_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        // As if earlier pipelines saved these with "as"
        resources.registerSavedName("saved_stream");
        resources.registerSavedName("saved_table");
    }

    private static final String STREAM_JOIN = """
            name: joinOp
            stream:
              topic: other
              keyType: string
              valueType: string
            valueJoiner:
              expression: value1
              resultType: string
            timeDifference: 10s
            grace: 5s
            thisStore:
              type: window
              name: thisStore
            otherStore:
              type: window
              name: otherStore
            """;

    private static final String TABLE_JOIN = """
            name: joinOp
            table: theTable
            valueJoiner:
              expression: value1
              resultType: string
            """;

    private static final String GLOBAL_TABLE_JOIN = """
            name: joinOp
            globalTable: theGlobalTable
            mapper:
              expression: key
              resultType: string
            valueJoiner:
              expression: value1
              resultType: string
            """;

    // Not defined under streams or tables, but saved with "as" in an earlier pipeline
    private static final String SAVED_STREAM_JOIN = """
            name: joinOp
            stream: saved_stream
            valueJoiner:
              expression: value1
              resultType: string
            timeDifference: 10s
            thisStore:
              type: window
              name: thisStore
            otherStore:
              type: window
              name: otherStore
            """;

    private static final String SAVED_TABLE_JOIN = """
            name: joinOp
            table: saved_table
            valueJoiner:
              expression: value1
              resultType: string
            """;

    static Stream<Arguments> joinCases() {
        final Function<TopologyResources, OperationParser<BaseOperation>> inner = JoinOperationParser::new;
        final Function<TopologyResources, OperationParser<BaseOperation>> left = LeftJoinOperationParser::new;
        final Function<TopologyResources, OperationParser<BaseOperation>> outer = OuterJoinOperationParser::new;
        return Stream.of(
                arguments("inner stream", inner, STREAM_JOIN, JoinWithStreamOperation.class),
                arguments("inner table", inner, TABLE_JOIN, JoinWithTableOperation.class),
                arguments("inner global table", inner, GLOBAL_TABLE_JOIN, JoinWithGlobalTableOperation.class),
                arguments("left stream", left, STREAM_JOIN, LeftJoinWithStreamOperation.class),
                arguments("left table", left, TABLE_JOIN, LeftJoinWithTableOperation.class),
                arguments("left global table", left, GLOBAL_TABLE_JOIN, LeftJoinWithGlobalTableOperation.class),
                arguments("outer stream", outer, STREAM_JOIN, OuterJoinWithStreamOperation.class),
                arguments("outer table", outer, TABLE_JOIN, OuterJoinWithTableOperation.class),
                arguments("inner saved stream", inner, SAVED_STREAM_JOIN, JoinWithStreamOperation.class),
                arguments("inner saved table", inner, SAVED_TABLE_JOIN, JoinWithTableOperation.class),
                arguments("left saved stream", left, SAVED_STREAM_JOIN, LeftJoinWithStreamOperation.class),
                arguments("left saved table", left, SAVED_TABLE_JOIN, LeftJoinWithTableOperation.class),
                arguments("outer saved stream", outer, SAVED_STREAM_JOIN, OuterJoinWithStreamOperation.class),
                arguments("outer saved table", outer, SAVED_TABLE_JOIN, OuterJoinWithTableOperation.class));
    }

    @ParameterizedTest(name = "parses {0} join")
    @DisplayName("each join parser variant parses its YAML into the expected join operation type")
    @MethodSource("joinCases")
    void parsesJoin(String description,
                    Function<TopologyResources, OperationParser<BaseOperation>> parserFactory,
                    String yaml,
                    Class<? extends BaseOperation> expectedType) throws Exception {
        final var operation = parserFactory.apply(resources).parse(nodeOf(yaml));
        assertThat(operation).isInstanceOf(expectedType);
    }

    @Test
    @DisplayName("inner, left and outer join parsers return null for a null node")
    void joinParsersReturnNullForNullNode() {
        assertThat(new JoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new LeftJoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new OuterJoinOperationParser(resources).parser().parse(null)).isNull();
    }

    static Stream<Arguments> unknownTargetCases() {
        final Function<TopologyResources, StructsParser<? extends BaseOperation>> inner = r -> new JoinOperationParser(r).parser();
        final Function<TopologyResources, StructsParser<? extends BaseOperation>> left = r -> new LeftJoinOperationParser(r).parser();
        final Function<TopologyResources, StructsParser<? extends BaseOperation>> outer = r -> new OuterJoinOperationParser(r).parser();
        final var unknownStream = SAVED_STREAM_JOIN.replace("saved_stream", "typo_stream");
        final var unknownTable = SAVED_TABLE_JOIN.replace("saved_table", "typo_table");
        return Stream.of(
                arguments("inner stream", inner, unknownStream, "Unknown stream \"typo_stream\". Define it under \"streams\", or save a stream with \"as: typo_stream\""),
                arguments("inner table", inner, unknownTable, "Unknown table \"typo_table\". Define it under \"tables\", or save a table with \"as: typo_table\""),
                arguments("left stream", left, unknownStream, "Unknown stream \"typo_stream\""),
                arguments("left table", left, unknownTable, "Unknown table \"typo_table\""),
                arguments("outer stream", outer, unknownStream, "Unknown stream \"typo_stream\""),
                arguments("outer table", outer, unknownTable, "Unknown table \"typo_table\""));
    }

    @ParameterizedTest(name = "rejects unknown {0}")
    @DisplayName("a name that is neither defined nor saved by an earlier pipeline is rejected when parsing")
    @MethodSource("unknownTargetCases")
    void rejectsUnknownTarget(String description,
                              Function<TopologyResources, StructsParser<? extends BaseOperation>> parserFactory,
                              String yaml,
                              String expectedMessage) throws Exception {
        final var parser = parserFactory.apply(resources);
        final var node = nodeOf(yaml);
        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    @DisplayName("a globalTable that is not defined is rejected, because a saved result can not be a globalTable")
    void rejectsGlobalTableThatIsNotDefined() throws Exception {
        final var node = nodeOf(GLOBAL_TABLE_JOIN.replace("theGlobalTable", "saved_table"));
        assertThatThrownBy(() -> new JoinOperationParser(resources).parser().parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown globalTable \"saved_table\"")
                .hasMessageContaining("join with it using \"table\" instead");
    }
}
