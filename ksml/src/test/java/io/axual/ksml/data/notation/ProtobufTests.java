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
        NotationTest.schemaTest("protobuf", new ProtobufSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTest.dataTest("protobuf", new ProtobufDataObjectMapper());
    }

    @Test
    void serdeTest() {
        final var notation = new ProtobufNotation("proto", ProtobufNotation.SerdeType.APICURIO, new NativeDataObjectMapper(), new HashMap<>(), new MockRegistryClient());
        NotationTest.serdeTest("protobuf", notation);
    }
}
