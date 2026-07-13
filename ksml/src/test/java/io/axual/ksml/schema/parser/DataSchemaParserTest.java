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

import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.exception.ParseException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.axual.ksml.schema.parser.SchemaParserTestSupport.nodeOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataSchemaParserTest {

    private final DataSchemaParser parser = new DataSchemaParser();

    @ParameterizedTest
    @DisplayName("parses each primitive type name into a non-null schema")
    @ValueSource(strings = {"any", "boolean", "byte", "short", "integer", "long", "float", "double", "bytes", "string"})
    void parsesPrimitiveTypes(String type) throws Exception {
        assertThat(parser.parse(nodeOf(type))).isNotNull();
    }

    @Test
    @DisplayName("parses the quoted \"null\" type name into a non-null schema")
    void parsesNullType() throws Exception {
        // Quoted so YAML treats it as the string "null" rather than a null node.
        assertThat(parser.parse(nodeOf("\"null\""))).isNotNull();
    }

    @Test
    @DisplayName("parsing a null node returns null")
    void returnsNullForNullNode() {
        assertThat(parser.parse(null)).isNull();
    }

    @Test
    @DisplayName("parses a fixed type definition into a FixedSchema")
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
    @DisplayName("parses an enum type with both scalar and object symbols into an EnumSchema")
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
    @DisplayName("parses a list type definition into a ListSchema")
    void parsesListSchema() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: list
                items: string
                """));
        assertThat(schema).isInstanceOf(ListSchema.class);
    }

    @Test
    @DisplayName("parses a map type definition into a MapSchema")
    void parsesMapSchema() throws Exception {
        final var schema = parser.parse(nodeOf("""
                type: map
                values: long
                """));
        assertThat(schema).isInstanceOf(MapSchema.class);
    }

    @Test
    @DisplayName("parses a struct type with a fully specified field into a StructSchema")
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
    @DisplayName("parses a top-level array of types into a UnionSchema")
    void parsesUnionSchemaFromArray() throws Exception {
        final var schema = parser.parse(nodeOf("""
                - string
                - long
                """));
        assertThat(schema).isInstanceOf(UnionSchema.class);
    }

    @Test
    @DisplayName("parsing an unrecognised type name throws ParseException")
    void rejectsUnknownSchemaType() {
        assertThatThrownBy(() -> parser.parse(nodeOf("type: bogus")))
                .isInstanceOf(ParseException.class);
    }

    @Test
    @DisplayName("parsing a recognised type with no schema branch throws ParseException about an unknown schema type")
    void rejectsTypeWithoutSwitchMapping() {
        // "tuple" is a recognised type but has no dedicated schema branch.
        assertThatThrownBy(() -> parser.parse(nodeOf("type: tuple")))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown schema type");
    }
}
