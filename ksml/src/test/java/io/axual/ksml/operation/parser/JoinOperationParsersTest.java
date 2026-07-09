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
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static io.axual.ksml.operation.parser.OperationParserTestSupport.nodeOf;
import static io.axual.ksml.operation.parser.OperationParserTestSupport.registerNotations;
import static org.assertj.core.api.Assertions.assertThat;
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
                arguments("outer table", outer, TABLE_JOIN, OuterJoinWithTableOperation.class));
    }

    @ParameterizedTest(name = "parses {0} join")
    @MethodSource("joinCases")
    void parsesJoin(String description,
                    Function<TopologyResources, OperationParser<BaseOperation>> parserFactory,
                    String yaml,
                    Class<? extends BaseOperation> expectedType) throws Exception {
        final var operation = parserFactory.apply(resources).parse(nodeOf(yaml));
        assertThat(operation).isInstanceOf(expectedType);
    }

    @Test
    void joinParsersReturnNullForNullNode() {
        assertThat(new JoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new LeftJoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new OuterJoinOperationParser(resources).parser().parse(null)).isNull();
    }
}
