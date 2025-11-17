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

import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.string.StringNotation;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.util.JsonNodeUtil;
import io.axual.ksml.schema.NativeDataSchemaMapper;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NotationTestRunner {
    public interface Tester<T> {
        void test(T input, T output);
    }

    static <T> void schemaTest(String type, DataSchemaMapper<T> schemaMapper, Tester<DataSchema> tester) {
        try {
            final var inputSchema = TestData.testSchema();
            final var nativeSchema = new NativeDataSchemaMapper().fromDataSchema(inputSchema);
            final var jsonSchema = JsonNodeUtil.convertNativeToJsonNode(nativeSchema).toPrettyString();
            System.out.println("INPUT SCHEMA: " + jsonSchema);
            final var typeSchema = schemaMapper.fromDataSchema(inputSchema);
            System.out.println(type.toUpperCase() + " SCHEMA: " + typeSchema);
            final var outputSchema = schemaMapper.toDataSchema(inputSchema.namespace(), inputSchema.name(), typeSchema);
            final var outputSchema2 = schemaMapper.fromDataSchema(outputSchema);
            System.out.println("OUTPUT SCHEMA: " + outputSchema2);
            tester.test(inputSchema, outputSchema);
        } catch (Exception e) {
            throw new SchemaException("Test failed", e);
        }
    }

    static <T> void schemaTest(String type, DataSchemaMapper<T> schemaMapper, EqualityFlags flags) {
        schemaTest(type, schemaMapper, (input, output) -> {
            final var inputEqualsOutput = input.equals(output, flags);
            if (!inputEqualsOutput.isEqual()) {
                System.out.println("INPUT NOT EQUAL TO OUTPUT: \n" + inputEqualsOutput.toString("  ", true));
            }
            final var outputEqualsInput = output.equals(input, flags);
            if (!outputEqualsInput.isEqual()) {
                System.out.println("OUTPUT NOT EQUAL TO INPUT: \n" + outputEqualsInput.toString("  ", true));
            }

            assertThat(input)
                    .as("Input schema should match output schema")
                    .returns(true, i -> i.equals(output, flags).isEqual())
                    .returns(true, o -> o.equals(input, flags).isEqual());
        });
    }

    static <T> void dataTest(String type, DataObjectMapper<T> objectMapper, EqualityFlags flags) {
        try {
            final var inputData = TestData.testStruct();
            System.out.println("INPUT DATA: " + inputData);
            final var nativeObject = objectMapper.fromDataObject(inputData);
            System.out.println(type.toUpperCase() + " DATA: " + nativeObject);
            final var outputData = objectMapper.toDataObject(inputData.type(), nativeObject);
            System.out.println("OUTPUT DATA: " + outputData);
            final var equal = inputData.equals(outputData, flags);
            assertTrue(equal.isEqual(), "Input data should match output data:\n" + equal.toString(true));
        } catch (Exception e) {
            throw new DataException("Test failed", e);
        }
    }

    static void serdeTest(Notation notation, boolean strictTypeChecking, EqualityFlags flags) {
        try {
            final var inputData = TestData.testStruct();
            System.out.println("INPUT DATA: " + inputData);
            final var serde = notation.serde(inputData.type(), false);
            final var headers = new RecordHeaders();
            final var serialized = serde.serializer().serialize("topic", headers, inputData);
            if (notation instanceof StringNotation) {
                final var serializedString = new StringDeserializer().deserialize("topic", serialized);
                System.out.println("SERIALIZED " + notation.name().toUpperCase() + " STRING: " + (serializedString != null ? serializedString : "null"));
            } else {
                System.out.println("SERIALIZED " + notation.name().toUpperCase() + " BYTES: " + new NativeDataObjectMapper().toDataObject(serialized).toString());
            }
            System.out.println("HEADERS: " + headers);
            var outputData = (DataObject) serde.deserializer().deserialize("topic", headers, serialized);
            System.out.println("OUTPUT DATA: " + outputData);
            if (!strictTypeChecking) {
                outputData = new DataObjectConverter().convert(null, outputData, new UserType(null, inputData.type()));
                System.out.println("CONVERTED OUTPUT DATA: " + outputData);
            }
            final var equal = inputData.equals(outputData, flags);
            assertTrue(equal.isEqual(), "Input data should match output data\n" + equal.toString(true));
        } catch (Exception e) {
            throw new DataException("Test failed", e);
        }
    }
}
