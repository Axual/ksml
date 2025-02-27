package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import io.axual.ksml.data.csv.TestData;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.csv.CsvDataObjectMapper;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.csv.CsvSchemaMapper;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvTests {

    @Test
    void schemaTest() {
        final var inputSchema = TestData.testSchema();
        final var schemaMapper = new CsvSchemaMapper();
        final var csvSchema = schemaMapper.fromDataSchema(inputSchema);
        System.out.println(csvSchema);
        final var outputSchema = (StructSchema) schemaMapper.toDataSchema(inputSchema.namespace(), inputSchema.name(), csvSchema);

        // Check the conversion
        final var inputFieldNames = inputSchema.fields().stream().map(DataField::name).toArray(String[]::new);
        final var outputFieldNames = outputSchema.fields().stream().map(DataField::name).toArray(String[]::new);
        assertArrayEquals(inputFieldNames, outputFieldNames, "Input schema field names should match output schema field names");
    }

    @Test
    void dataTest() {
        final var inputData = TestData.testStruct();
        System.out.println(inputData);
        final var objectMapper = new CsvDataObjectMapper();
        final var csvData = objectMapper.fromDataObject(inputData);
        System.out.println(csvData);
        final var outputData = objectMapper.toDataObject(inputData.type(), csvData);
        System.out.println(outputData);
        assertEquals(inputData, outputData, "Input data should match output data");
    }

    @Test
    void serdeTest() {
        final var notation = new CsvNotation("csv", new NativeDataObjectMapper());
        final var inputData = TestData.testStruct();
        final var serde = notation.serde(inputData.type(), false);
        final var serialized = serde.serializer().serialize("topic", inputData);
        final var outputData = serde.deserializer().deserialize("topic", serialized);
        assertEquals(inputData, outputData, "Input data should match output data");
    }
}
