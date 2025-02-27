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

import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.AvroSchemaMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class AvroTests {
    @Test
    void schemaTest() {
        NotationTest.schemaTest("avro", new AvroSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTest.dataTest("avro", new AvroDataObjectMapper());
    }

    @Test
    void serdeTest() {
//        final var notation = new AvroNotation("avro", AvroNotation.SerdeType.APICURIO, new AvroDataObjectMapper(), new HashMap<>(), new MockRegistryClient());
//        NotationTest.serdeTest("avro", notation);
    }
}
