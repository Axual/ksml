package io.axual.ksml.notation.protobuf;

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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.protobuf.ProtobufSchemaMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtobufTests {
    @Test
    void schemaTest() {
        final var inputSchema = TestData.testSchema();
        final var schemaMapper = new ProtobufSchemaMapper();
        final var protoSchema = schemaMapper.fromDataSchema(inputSchema);
        System.out.println(protoSchema.getProtoFileElement().toSchema());
        final var outputSchema = schemaMapper.toDataSchema(inputSchema.namespace(), inputSchema.name(), protoSchema);
        assertEquals(inputSchema, outputSchema, "Input schema should match output schema");
    }

    @Test
    void dataTest() {
        final var inputData = TestData.testStruct();
        final var objectMapper = new ProtobufDataObjectMapper();
        final var protoData = objectMapper.fromDataObject(inputData);
        System.out.println(protoData.toString());
        final var outputData = objectMapper.toDataObject(inputData.type(), protoData);
        assertEquals(inputData, outputData, "Input data should match output data");
    }

    @Test
    void serdeTest() {
        final var inputData = TestData.testStruct();
        final var configs = new HashMap<String, Object>();
        configs.put("apicurio.registry.url", "http://localhost:8080");
        final var notation = new ProtobufNotation("proto", ProtobufNotation.SerdeType.APICURIO, new NativeDataObjectMapper(), configs, new MockRegistryClient());
        final var serde = notation.serde(inputData.type(), false);
        final var serialized = serde.serializer().serialize("topic", inputData);
        final var outputData = serde.deserializer().deserialize("topic", serialized);
        assertEquals(inputData, outputData, "Input data should match output data");
    }
}
