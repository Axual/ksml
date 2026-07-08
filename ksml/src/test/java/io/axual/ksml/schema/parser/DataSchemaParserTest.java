package io.axual.ksml.schema.parser;

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
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataSchemaParserTest {

    private final DataSchemaParser parser = new DataSchemaParser();

    private static ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    @ParameterizedTest
    @ValueSource(strings = {"any", "boolean", "byte", "short", "integer", "long", "float", "double", "bytes", "string"})
    void parsesPrimitiveTypes(String type) throws Exception {
        assertThat(parser.parse(nodeOf(type))).isNotNull();
    }

    @Test
    void parsesNullType() throws Exception {
        // Quoted so YAML treats it as the string "null" rather than a null node.
        assertThat(parser.parse(nodeOf("\"null\""))).isNotNull();
    }

    @Test
    void returnsNullForNullNode() {
        assertThat(parser.parse(null)).isNull();
    }

    @Test
    void parsesFixedSchema() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: fixed
                name: Hash
                namespace: com.example
                size: 16
                """));
        assertThat(schema).isInstanceOf(FixedSchema.class);
    }

    @Test
    void parsesEnumSchemaWithScalarAndObjectSymbols() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: enum
                name: Color
                namespace: com.example
                symbols:
                  - RED
                  - name: GREEN
                    doc: the green one
                    tag: 2
                """));
        assertThat(schema).isInstanceOf(EnumSchema.class);
    }

    @Test
    void parsesListSchema() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: list
                items: string
                """));
        assertThat(schema).isInstanceOf(ListSchema.class);
    }

    @Test
    void parsesMapSchema() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: map
                values: long
                """));
        assertThat(schema).isInstanceOf(MapSchema.class);
    }

    @Test
    void parsesStructSchemaWithRichField() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: struct
                name: Person
                namespace: com.example
                doc: A person
                fields:
                  - name: age
                    type: integer
                    doc: the age
                    required: true
                    default: 0
                    order: ASCENDING
                """));
        assertThat(schema).isInstanceOf(StructSchema.class);
        assertThat(((StructSchema) schema).fields()).hasSize(1);
    }

    @Test
    void parsesUnionSchemaFromArray() throws Exception {
        final var schema = parser.parse(nodeOf("""
                - string
                - long
                """));
        assertThat(schema).isInstanceOf(UnionSchema.class);
    }

    @Test
    void rejectsUnknownSchemaType() {
        assertThatThrownBy(() -> parser.parse(nodeOf("type: bogus")))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void rejectsTypeWithoutSwitchMapping() {
        // "tuple" is a recognised type but has no dedicated schema branch.
        assertThatThrownBy(() -> parser.parse(nodeOf("type: tuple")))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown schema type");
    }
}
