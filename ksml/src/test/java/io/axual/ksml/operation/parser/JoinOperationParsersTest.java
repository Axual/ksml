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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.operation.JoinWithGlobalTableOperation;
import io.axual.ksml.operation.JoinWithStreamOperation;
import io.axual.ksml.operation.JoinWithTableOperation;
import io.axual.ksml.operation.LeftJoinWithGlobalTableOperation;
import io.axual.ksml.operation.LeftJoinWithStreamOperation;
import io.axual.ksml.operation.LeftJoinWithTableOperation;
import io.axual.ksml.operation.OuterJoinWithStreamOperation;
import io.axual.ksml.operation.OuterJoinWithTableOperation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoinOperationParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void setUp() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
        resources.register("theTable",
                new TableDefinition("the_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
        resources.register("theGlobalTable",
                new GlobalTableDefinition("the_global_table", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null, null));
    }

    private static ParseNode nodeOf(String yaml) throws Exception {
        return ParseNode.fromRoot(YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class), "test");
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

    // --- inner join ------------------------------------------------------------------------------

    @Test
    void parsesStreamJoin() throws Exception {
        assertThat(new JoinOperationParser(resources).parser().parse(nodeOf(STREAM_JOIN)))
                .isInstanceOf(JoinWithStreamOperation.class);
    }

    @Test
    void parsesTableJoin() throws Exception {
        assertThat(new JoinOperationParser(resources).parser().parse(nodeOf(TABLE_JOIN)))
                .isInstanceOf(JoinWithTableOperation.class);
    }

    @Test
    void parsesGlobalTableJoin() throws Exception {
        assertThat(new JoinOperationParser(resources).parser().parse(nodeOf(GLOBAL_TABLE_JOIN)))
                .isInstanceOf(JoinWithGlobalTableOperation.class);
    }

    // --- left join -------------------------------------------------------------------------------

    @Test
    void parsesLeftStreamJoin() throws Exception {
        assertThat(new LeftJoinOperationParser(resources).parser().parse(nodeOf(STREAM_JOIN)))
                .isInstanceOf(LeftJoinWithStreamOperation.class);
    }

    @Test
    void parsesLeftTableJoin() throws Exception {
        assertThat(new LeftJoinOperationParser(resources).parser().parse(nodeOf(TABLE_JOIN)))
                .isInstanceOf(LeftJoinWithTableOperation.class);
    }

    @Test
    void parsesLeftGlobalTableJoin() throws Exception {
        assertThat(new LeftJoinOperationParser(resources).parser().parse(nodeOf(GLOBAL_TABLE_JOIN)))
                .isInstanceOf(LeftJoinWithGlobalTableOperation.class);
    }

    // --- outer join ------------------------------------------------------------------------------

    @Test
    void parsesOuterStreamJoin() throws Exception {
        assertThat(new OuterJoinOperationParser(resources).parser().parse(nodeOf(STREAM_JOIN)))
                .isInstanceOf(OuterJoinWithStreamOperation.class);
    }

    @Test
    void parsesOuterTableJoin() throws Exception {
        assertThat(new OuterJoinOperationParser(resources).parser().parse(nodeOf(TABLE_JOIN)))
                .isInstanceOf(OuterJoinWithTableOperation.class);
    }

    @Test
    void joinParsersReturnNullForNullNode() {
        assertThat(new JoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new LeftJoinOperationParser(resources).parser().parse(null)).isNull();
        assertThat(new OuterJoinOperationParser(resources).parser().parse(null)).isNull();
    }
}
