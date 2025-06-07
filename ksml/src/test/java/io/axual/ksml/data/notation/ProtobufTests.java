package io.axual.ksml.data.notation;

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

class ProtobufTests {
    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest("protobuf", new ProtobufSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest("protobuf", new ProtobufDataObjectMapper());
    }

    @Test
    void serdeTest() {
        final var notation = new ProtobufNotation("proto", ProtobufNotation.SerdeType.APICURIO, new NativeDataObjectMapper(), new HashMap<>(), new MockRegistryClient());
        NotationTestRunner.serdeTest("protobuf", notation, true);
    }

    void extraTest() {
        // This method checks for code completeness of ProtobufSchemaMapper. It will warn if schema translation to
        // DataSchema and back gives deltas between ProtobufSchemas.
//        final var proto = new io.apicurio.registry.serde.protobuf.ProtobufSchemaParser<>().parseSchema(schemaIn.getBytes(), Collections.emptyMap());
//        final var internalSchema = MAPPER.toDataSchema(name, proto);
//        final var out = MAPPER.fromDataSchema(internalSchema);
//        final var outFe = out.getProtoFileElement();
//        final var schemaOut = outFe.toSchema();
//        final var checker = new ProtobufCompatibilityCheckerLibrary(new ProtobufFile(schemaIn), new ProtobufFile(schemaOut));
//        final var diffs = checker.findDifferences();
//        assertTrue(diffs.isEmpty(), "PROTOBUF schema {} in/out is not compatible: " + name);
    }
}
