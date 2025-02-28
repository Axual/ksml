package io.axual.ksml.data.notation;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.DataSchema;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NotationTest {
    public interface Tester<T> {
        void test(T input, T output);
    }

    static <T> void schemaTest(String type, DataSchemaMapper<T> schemaMapper, Tester<DataSchema> tester) {
        try {
            final var inputSchema = TestData.testSchema();
            System.out.println("INPUT SCHEMA: " + inputSchema);
            final var avroSchema = schemaMapper.fromDataSchema(inputSchema);
            System.out.println(type.toUpperCase() + " SCHEMA: " + avroSchema);
            final var outputSchema = schemaMapper.toDataSchema(inputSchema.namespace(), inputSchema.name(), avroSchema);
            System.out.println("OUTPUT SCHEMA: " + outputSchema);
            tester.test(inputSchema, outputSchema);
        } catch (Exception e) {
            throw new SchemaException("Test failed", e);
        }
    }

    static <T> void schemaTest(String type, DataSchemaMapper<T> schemaMapper) {
        schemaTest(type, schemaMapper, (input, output) -> {
            assertEquals(input, output, "Input schema should match output schema");
        });
    }

    static <T> void dataTest(String type, DataObjectMapper<T> objectMapper) {
        try {
            final var inputData = TestData.testStruct();
            System.out.println("INPUT DATA: " + inputData);
            final var nativeObject = objectMapper.fromDataObject(inputData);
            System.out.println(type.toUpperCase() + " DATA: " + nativeObject);
            final var outputData = objectMapper.toDataObject(inputData.type(), nativeObject);
            System.out.println("OUTPUT DATA: " + outputData);
            assertEquals(inputData, outputData, "Input data should match output data");
        } catch (Exception e) {
            throw new DataException("Test failed", e);
        }
    }

    static void serdeTest(String type, Notation notation) {
        try {
            final var inputData = TestData.testStruct();
            System.out.println("INPUT DATA: " + inputData);
            final var serde = notation.serde(inputData.type(), false);
            final var headers = new RecordHeaders();
            final var serialized = serde.serializer().serialize("topic", headers, inputData);
            System.out.println("SERIALIZED " + type.toUpperCase() + " BYTES: " + new NativeDataObjectMapper().toDataObject(serialized).toString());
            System.out.println("HEADERS: " + headers);
            final var outputData = serde.deserializer().deserialize("topic", headers, serialized);
            System.out.println("OUTPUT DATA: " + outputData);
            assertEquals(inputData, outputData, "Input data should match output data");
        } catch (Exception e) {
            throw new DataException("Test failed", e);
        }
    }
}
