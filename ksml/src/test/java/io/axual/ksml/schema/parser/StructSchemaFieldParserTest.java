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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.schema.parser.SchemaParserTestSupport.nodeOf;
import static org.assertj.core.api.Assertions.assertThat;

class StructSchemaFieldParserTest {

    private final StructSchemaFieldParser parser = new StructSchemaFieldParser();

    @Test
    void parsesMinimalFieldAsRequired() throws Exception {
        final var field = parser.parse(nodeOf("""
                name: id
                type: string
                """));
        assertThat(field.name()).isEqualTo("id");
        assertThat(field.required()).isTrue();
    }

    @Test
    void honoursExplicitRequiredFlag() throws Exception {
        final var field = parser.parse(nodeOf("""
                name: id
                type: string
                required: false
                """));
        assertThat(field.required()).isFalse();
    }

    @Test
    void parsesConstantTagDefaultAndOrder() throws Exception {
        final var field = parser.parse(nodeOf("""
                name: id
                type: string
                doc: identifier
                constant: true
                tag: 7
                defaultValue: fallback
                order: DESCENDING
                """));
        assertThat(field.constant()).isTrue();
        assertThat(field.tag()).isEqualTo(7);
        assertThat(field.defaultValue()).isNotNull();
    }

    @Test
    void unionWithoutNullStaysRequired() throws Exception {
        final var field = parser.parse(nodeOf("""
                name: id
                type:
                  - string
                  - long
                """));
        assertThat(field.schema()).isInstanceOf(UnionSchema.class);
        assertThat(field.required()).isTrue();
    }

    @Test
    void optionalUnionWithNullBecomesNotRequiredAndStripsNull() throws Exception {
        final var field = parser.parse(nodeOf("""
                name: id
                type:
                  - "null"
                  - string
                """));
        assertThat(field.required()).isFalse();
        assertThat(field.schema()).isInstanceOfSatisfying(UnionSchema.class,
                union -> assertThat(union.contains(DataSchema.NULL_SCHEMA)).isFalse());
    }
}
