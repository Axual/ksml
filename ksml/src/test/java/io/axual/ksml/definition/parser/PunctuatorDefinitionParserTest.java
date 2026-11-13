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
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PunctuatorDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PunctuatorDefinitionParserTest {
    @BeforeEach
    void registerNotations() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
    }

    private static ParseNode nodeOf(String yaml) {
        return ParseNode.fromRoot(YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class), "test");
    }

    private static void assertPunctuator(FunctionDefinition definition) {
        assertThat(definition).isInstanceOf(PunctuatorDefinition.class);
        assertThat(definition.type()).isEqualTo(KSMLDSL.Functions.TYPE_PUNCTUATOR);
        assertThat(definition.resultType()).isEqualTo(UserType.UNKNOWN);
        assertThat(definition.parameters()).containsExactly(
                new ParameterDefinition("timestamp", DataLong.DATATYPE),
                new ParameterDefinition("limit", DataLong.DATATYPE, true, "10"));
        assertThat(definition.globalCode()).containsExactly("count = 0");
        assertThat(definition.storeNames()).isEqualTo(List.of("totals"));
    }

    @Test
    @DisplayName("a named function with type punctuator parses with globalCode, stores and custom parameters")
    void namedPunctuator() {
        final var definition = new TypedFunctionDefinitionParser().parse(nodeOf("""
                type: punctuator
                globalCode: count = 0
                code: context.forward("a", 1)
                stores: [totals]
                parameters:
                  - name: limit
                    type: long
                    defaultValue: "10"
                """));
        assertPunctuator(definition);
    }

    @Test
    @DisplayName("an inline punctuator parses without a type")
    void inlinePunctuator() {
        final var definition = new PunctuatorDefinitionParser(false).parser().parse(nodeOf("""
                globalCode: count = 0
                code: context.forward("a", 1)
                stores: [totals]
                parameters:
                  - name: limit
                    type: long
                    defaultValue: "10"
                """));
        assertPunctuator(definition);
    }

    @Test
    @DisplayName("a punctuator with a resultType is rejected, because it returns no result")
    void resultTypeRejected() {
        final var parser = new TypedFunctionDefinitionParser();
        final var node = nodeOf("""
                type: punctuator
                code: context.forward("a", 1)
                resultType: string
                """);
        assertThatThrownBy(() -> parser.parse(node))
                .hasMessageContaining("Function type 'punctuator' can not return a result");
    }

}
