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
import io.axual.ksml.notation.MockApicurioSchemaRegistryClient;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.protobuf.ProtobufSchemaMapper;
import io.axual.ksml.data.notation.protobuf.apicurio.ApicurioProtobufFileElementDescriptorMapper;
import io.axual.ksml.data.notation.protobuf.apicurio.ApicurioProtobufNotationProvider;
import io.axual.ksml.data.notation.protobuf.confluent.ConfluentProtobufFileElementDescriptorMapper;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_ENUM_SCHEMA_DEFAULT_VALUE;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_SCHEMA_DOC;

class ProtobufTests {
    private static final EqualityFlags PROTOBUF_EQUALITY_FLAGS = new EqualityFlags(
            IGNORE_ENUM_SCHEMA_DEFAULT_VALUE,
            IGNORE_STRUCT_SCHEMA_DOC
    );
    private final NotationTestRunner runner = new NotationTestRunner(TestData.Variant.PROTOBUF);

    @Test
    void apicurioSchemaTest() {
        runner.schemaTest(ProtobufNotation.NOTATION_NAME, new ProtobufSchemaMapper(new ApicurioProtobufFileElementDescriptorMapper()), PROTOBUF_EQUALITY_FLAGS);
    }

    @Test
    void confluentSchemaTest() {
        runner.schemaTest(ProtobufNotation.NOTATION_NAME, new ProtobufSchemaMapper(new ConfluentProtobufFileElementDescriptorMapper()), PROTOBUF_EQUALITY_FLAGS);
    }

    @Test
    void apicurioDataTest() {
        runner.dataTest(ProtobufNotation.NOTATION_NAME, new ProtobufDataObjectMapper(new ApicurioProtobufFileElementDescriptorMapper()), PROTOBUF_EQUALITY_FLAGS);
    }

    @Test
    void apicurioSerdeTest() {
        final var registryClient = new MockApicurioSchemaRegistryClient();
        final var provider = new ApicurioProtobufNotationProvider(registryClient);
        final var notationContext = new NotationContext(registryClient.configs());
        final var notation = provider.createNotation(notationContext);
        runner.serdeTest(notation, true, PROTOBUF_EQUALITY_FLAGS);
    }
}
