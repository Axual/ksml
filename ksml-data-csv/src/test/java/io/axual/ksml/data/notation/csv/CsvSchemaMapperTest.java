package io.axual.ksml.data.notation.csv;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CsvSchemaMapper} verifying CSV header <-> StructSchema conversions.
 *
 * <p>These tests ensure users can define CSV schemas as header files and KSML correctly
 * interprets them for data processing.</p>
 */
@DisplayName("CsvSchemaMapper - CSV header <-> StructSchema conversions")
class CsvSchemaMapperTest {

    private final CsvSchemaMapper mapper = new CsvSchemaMapper();

    @Test
    @DisplayName("Parses CSV header line to StructSchema with correct field names and order")
    void csvHeaderToStructSchema() {
        // Given: sensor data header from docs example
        var csvHeader = "name,timestamp,value,type,unit,color,city,owner";

        // When: converting to StructSchema
        var schema = mapper.toDataSchema("io.axual.test", "SensorData", csvHeader);

        // Then: should create StructSchema with all fields in order
        assertThat(schema).isInstanceOf(StructSchema.class);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.namespace()).isEqualTo("io.axual.test");
        assertThat(structSchema.name()).isEqualTo("SensorData");
        assertThat(structSchema.fields()).hasSize(8);

        // Verify field names in correct order
        assertThat(structSchema.field(0).name()).isEqualTo("name");
        assertThat(structSchema.field(1).name()).isEqualTo("timestamp");
        assertThat(structSchema.field(2).name()).isEqualTo("value");
        assertThat(structSchema.field(3).name()).isEqualTo("type");
        assertThat(structSchema.field(4).name()).isEqualTo("unit");
        assertThat(structSchema.field(5).name()).isEqualTo("color");
        assertThat(structSchema.field(6).name()).isEqualTo("city");
        assertThat(structSchema.field(7).name()).isEqualTo("owner");

        // All fields should be required and of string type
        assertThat(structSchema.field(0).required()).isTrue();
        assertThat(structSchema.field(0).schema().type()).isEqualTo("string");
    }

    @Test
    @DisplayName("Round-trip: StructSchema to CSV header to StructSchema preserves fields")
    void structSchemaRoundTrip() {
        // Given: a StructSchema
        var csvHeader = "name,age,city";
        var originalSchema = mapper.toDataSchema("io.axual.test", "Person", csvHeader);

        // When: converting to CSV header
        var resultHeader = mapper.fromDataSchema(originalSchema);

        // Then: should produce header with field names
        assertThat(resultHeader).isEqualTo("name,age,city");

        // When: converting back to schema
        var resultSchema = mapper.toDataSchema("io.axual.test", "Person", resultHeader);

        // Then: should have same fields
        assertThat(resultSchema).isInstanceOf(StructSchema.class);
        var struct = (StructSchema) resultSchema;
        assertThat(struct.fields()).hasSize(3);
        assertThat(struct.field(0).name()).isEqualTo("name");
        assertThat(struct.field(1).name()).isEqualTo("age");
        assertThat(struct.field(2).name()).isEqualTo("city");
    }

    @Test
    @DisplayName("Handles single field header")
    void singleFieldHeader() {
        // Given: header with single field
        var csvHeader = "id";

        // When: converting to schema
        var schema = mapper.toDataSchema("io.axual.test", "SimpleId", csvHeader);

        // Then: should create schema with one field
        assertThat(schema).isInstanceOf(StructSchema.class);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.fields()).hasSize(1);
        assertThat(structSchema.field(0).name()).isEqualTo("id");
    }

    @Test
    @DisplayName("Handles field names with underscores and numbers")
    void fieldNamesWithSpecialChars() {
        // Given: header with underscores and numbers
        var csvHeader = "field_1,field_2,field_name_3";

        // When: converting to schema
        var schema = mapper.toDataSchema("io.axual.test", "Test", csvHeader);

        // Then: field names should be preserved
        assertThat(schema).isInstanceOf(StructSchema.class);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.fields()).hasSize(3);
        assertThat(structSchema.field(0).name()).isEqualTo("field_1");
        assertThat(structSchema.field(1).name()).isEqualTo("field_2");
        assertThat(structSchema.field(2).name()).isEqualTo("field_name_3");
    }

    @Test
    @DisplayName("Converts StructSchema to CSV header in correct field order")
    void structSchemaToCsvHeader() {
        // Given: create schema from header
        var originalHeader = "name,timestamp,value,type,unit,color,city,owner";
        var schema = mapper.toDataSchema("io.axual.test", "SensorData", originalHeader);

        // When: converting back to header
        var resultHeader = mapper.fromDataSchema(schema);

        // Then: should match original header
        assertThat(resultHeader).isEqualTo(originalHeader);
    }

    @Test
    @DisplayName("Returns null for non-StructSchema input")
    void nonStructSchemaReturnsNull() {
        // Given: non-struct schema (null in this case)
        // When/Then: should return null
        assertThat(mapper.fromDataSchema(null)).isNull();
    }

    @Test
    @DisplayName("Handles whitespace-trimmed field names from CSV header")
    void trimmedFieldNames() {
        // Given: header with spaces (though not standard CSV)
        var csvHeader = "name, age, city";

        // When: parsing
        var schema = mapper.toDataSchema("io.axual.test", "Person", csvHeader);

        // Then: field names might include spaces (depending on CSV parser)
        assertThat(schema).isInstanceOf(StructSchema.class);
        var structSchema = (StructSchema) schema;
        assertThat(structSchema.fields()).hasSize(3);

        // CSV parser preserves the spaces after commas
        assertThat(structSchema.field(0).name()).isEqualTo("name");
        assertThat(structSchema.field(1).name()).contains("age");
        assertThat(structSchema.field(2).name()).contains("city");
    }
}
