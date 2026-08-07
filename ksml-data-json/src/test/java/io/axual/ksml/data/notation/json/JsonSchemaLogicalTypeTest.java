package io.axual.ksml.data.notation.json;

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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.LogicalSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.schema.logical.LogicalTypeConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonSchemaLogicalTypeTest {
    private final JsonSchemaMapper mapper = new JsonSchemaMapper(false);

    @Test
    @DisplayName("A string with format uuid maps to a uuid logical schema; a plain string stays a string")
    void readsUuidFormatAsLogicalSchema() {
        final var json = """
                {"title":"T","type":"object","properties":{
                  "id":{"type":"string","format":"uuid"},
                  "name":{"type":"string"}
                }}""";
        final var schema = (StructSchema) mapper.toDataSchema("ns", "T", json);
        assertThat(schema.field("id").schema()).isInstanceOf(LogicalSchema.class);
        assertThat(((LogicalSchema) schema.field("id").schema()).logicalType().name()).isEqualTo("uuid");
        assertThat(schema.field("name").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
    }

    @Test
    @DisplayName("A uuid logical schema is written as a string with format uuid")
    void writesUuidLogicalSchemaAsFormat() {
        final var schema = StructSchema.builder().namespace("ns").name("T")
                .field(new StructSchema.Field("id", new LogicalSchema(LogicalTypeConstants.UUID_TYPE)))
                .additionalFieldsAllowed(false)
                .build();
        final var json = mapper.fromDataSchema(schema);
        assertThat(json).contains("\"format\":\"uuid\"").contains("\"type\":\"string\"");
    }

    @Test
    @DisplayName("A uuid format round-trips through KSML and back to JSON Schema")
    void roundTripsUuidFormat() {
        final var json = """
                {"title":"T","type":"object","properties":{"id":{"type":"string","format":"uuid"}}}""";
        final var schema = mapper.toDataSchema("ns", "T", json);
        final var back = mapper.fromDataSchema(schema);
        final var reparsed = mapper.toDataSchema("ns", "T", back);
        assertThat(reparsed).isEqualTo(schema);
    }

    @Test
    @DisplayName("A non-uuid logical schema is written as its representation primitive, not a format")
    void writesNonUuidLogicalAsRepresentationPrimitive() {
        final var schema = StructSchema.builder().namespace("ns").name("T")
                .field(new StructSchema.Field("amount", new LogicalSchema(new DecimalLogicalType(10, 2))))
                .field(new StructSchema.Field("day", new LogicalSchema(LogicalTypeConstants.DATE_TYPE)))
                .additionalFieldsAllowed(false)
                .build();
        final var back = (StructSchema) mapper.toDataSchema("ns", "T", mapper.fromDataSchema(schema));
        assertThat(back.field("amount").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(back.field("day").schema()).isNotInstanceOf(LogicalSchema.class);
    }
}
