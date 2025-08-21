package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import org.apache.avro.Schema;
import org.apache.avro.SchemaFormatter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.schema.DataSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Named.named;

class AvroSchemaParserTest {
    final AvroSchemaParser schemaParser = new AvroSchemaParser();
    static final AvroSchemaMapper SCHEMA_MAPPER = new AvroSchemaMapper();

    @Test
    @DisplayName("Wrong parse type exception")
    void parseIncorrectSchema() {
        final var avroStringSchema = SchemaFormatter.format("json/pretty",Schema.create(Schema.Type.STRING));
        assertThatCode(()->schemaParser.parse("TEST_CONTEXT","string", avroStringSchema))
                .isInstanceOf(SchemaException.class);
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Verify schema parsing")
    void parseCorrectSchemaTypes(String name, String schemaContent, DataSchema expectedSchema) {
        assertThat(schemaParser.parse("TEST_CONTEXT", name, schemaContent))
                .as("Parse schema %s", name)
                .isEqualTo(expectedSchema);
    }

    public static Stream<Arguments> parseCorrectSchemaTypes() {
        final var namespace = "io.axual.test";
        final var arraysName = "Arrays";
        final var arraysSchemaString = AvroTestUtil.loadResourceToString(AvroTestUtil.SCHEMA_ARRAYS);
        final var arraysRecordSchema = AvroTestUtil.loadSchema(AvroTestUtil.SCHEMA_ARRAYS);
        final var arraysStructSchema = SCHEMA_MAPPER.toDataSchema(namespace, arraysName, arraysRecordSchema);

        final var collectionsName = "Arrays";
        final var collectionsSchemaString = AvroTestUtil.loadResourceToString(AvroTestUtil.SCHEMA_COLLECTIONS);
        final var collectionsRecordSchema = AvroTestUtil.loadSchema(AvroTestUtil.SCHEMA_COLLECTIONS);
        final var collectionsStructSchema = SCHEMA_MAPPER.toDataSchema(namespace, collectionsName, collectionsRecordSchema);

        return Stream.of(
                Arguments.of(named("Array Record", arraysName), arraysSchemaString, arraysStructSchema),
                Arguments.of(named("Collections Record", collectionsName), collectionsSchemaString, collectionsStructSchema)
        );
    }

}
