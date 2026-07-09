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

import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ConvertKeyValueOperation;
import io.axual.ksml.operation.FilterNotOperation;
import io.axual.ksml.operation.MergeOperation;
import io.axual.ksml.operation.ReduceOperation;
import io.axual.ksml.operation.SuppressOperation;
import io.axual.ksml.operation.WindowBySessionOperation;
import io.axual.ksml.operation.WindowByTimeOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.axual.ksml.operation.parser.OperationParserTestSupport.nodeOf;
import static io.axual.ksml.operation.parser.OperationParserTestSupport.registerNotations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class OperationParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void setUp() {
        registerNotations();
    }

    static Stream<Arguments> validTimeWindows() {
        return Stream.of(
                arguments("tumbling", "windowType: tumbling\nduration: 10s"),
                arguments("hopping", "windowType: hopping\nduration: 10s\nadvanceBy: 5s"),
                arguments("hopping with grace", "windowType: hopping\nduration: 10s\nadvanceBy: 5s\ngrace: 2s"),
                arguments("tumbling with grace", "windowType: tumbling\nduration: 10s\ngrace: 2s"),
                arguments("sliding", "windowType: sliding\ntimeDifference: 10s\ngrace: 5s"));
    }

    @ParameterizedTest(name = "parses {0} time window")
    @MethodSource("validTimeWindows")
    void parsesTimeWindow(String description, String yaml) throws Exception {
        final var op = new WindowByTimeOperationParser(resources).parser().parse(nodeOf(yaml));
        assertThat(op).isInstanceOf(WindowByTimeOperation.class);
    }

    @Test
    void rejectsHoppingWindowThatAdvancesByMoreThanItsDuration() throws Exception {
        final var parser = new WindowByTimeOperationParser(resources).parser();
        final var node = nodeOf("windowType: hopping\nduration: 10s\nadvanceBy: 20s");
        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("advanceBy");
    }

    @Test
    void parsesSessionWindow() throws Exception {
        final var op = new WindowBySessionOperationParser(resources).parser()
                .parse(nodeOf("inactivityGap: 10s\ngrace: 5s"));
        assertThat(op).isInstanceOf(WindowBySessionOperation.class);
    }

    @Test
    void parsesSuppressUntilTimeLimit() throws Exception {
        final var op = new SuppressOperationParser(resources).parser()
                .parse(nodeOf("until: timeLimit\nduration: 10s\nmaxRecords: 100\nbufferFullStrategy: emitEarlyWhenFull"));
        assertThat(op).isInstanceOf(SuppressOperation.class);
    }

    @Test
    void parsesSuppressUntilWindowCloses() throws Exception {
        final var op = new SuppressOperationParser(resources).parser()
                .parse(nodeOf("until: windowCloses\nbufferFullStrategy: shutdownWhenFull"));
        assertThat(op).isInstanceOf(SuppressOperation.class);
    }

    @Test
    void parsesReduceWithReducer() throws Exception {
        final var op = new ReduceOperationParser(resources).parser()
                .parse(nodeOf("name: reduceOp\nreducer:\n  expression: value1\n  resultType: string"));
        assertThat(op).isInstanceOf(ReduceOperation.class);
    }

    @Test
    void parsesReduceWithAdderAndSubtractor() throws Exception {
        final var op = new ReduceOperationParser(resources).parser()
                .parse(nodeOf("name: reduceOp\nadder:\n  expression: value1\n  resultType: string\nsubtractor:\n  expression: value2\n  resultType: string"));
        assertThat(op).isInstanceOf(ReduceOperation.class);
    }

    @Test
    void parsesMerge() throws Exception {
        final var op = new MergeOperationParser(resources).parser()
                .parse(nodeOf("stream:\n  topic: other\n  keyType: string\n  valueType: string"));
        assertThat(op).isInstanceOf(MergeOperation.class);
    }

    @Test
    void parsesFilterNot() throws Exception {
        final var op = new FilterNotOperationParser(resources).parser()
                .parse(nodeOf("name: filterNotOp\nif:\n  expression: \"true\""));
        assertThat(op).isInstanceOf(FilterNotOperation.class);
    }

    @Test
    void parsesConvertKeyValue() throws Exception {
        final var op = new ConvertKeyValueOperationParser(resources).parser()
                .parse(nodeOf("into: \"(string, string)\""));
        assertThat(op).isInstanceOf(ConvertKeyValueOperation.class);
    }
}
