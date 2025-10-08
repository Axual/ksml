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

import io.axual.ksml.data.notation.jsonschema.apicurio.MockApicurioSchemaRegistryClient;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.protobuf.ProtobufSchemaMapper;
import io.axual.ksml.data.notation.protobuf.apicurio.ApicurioProtobufDescriptorFileElementMapper;
import io.axual.ksml.data.notation.protobuf.apicurio.ApicurioProtobufNotationProvider;
import io.axual.ksml.data.notation.protobuf.confluent.ConfluentProtobufDescriptorFileElementMapper;
import io.axual.ksml.data.type.Flags;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.type.EqualityFlags.IGNORE_DATA_FIELD_DOC;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_NAMED_SCHEMA_DOC;

class ProtobufTests {
    private static final Flags PROTOBUF_FLAGS = new Flags(
            IGNORE_DATA_FIELD_DOC,
            IGNORE_NAMED_SCHEMA_DOC
    );

    @Test
    void apicurioSchemaTest() {
        NotationTestRunner.schemaTest(ProtobufNotation.NOTATION_NAME, new ProtobufSchemaMapper(new ApicurioProtobufDescriptorFileElementMapper()));
    }

    @Test
    void confluentSchemaTest() {
        NotationTestRunner.schemaTest(ProtobufNotation.NOTATION_NAME, new ProtobufSchemaMapper(new ConfluentProtobufDescriptorFileElementMapper()));
    }

    @Test
    void apicurioDataTest() {
        NotationTestRunner.dataTest(ProtobufNotation.NOTATION_NAME, new ProtobufDataObjectMapper(new ApicurioProtobufDescriptorFileElementMapper()), PROTOBUF_FLAGS);
    }

    @Test
    void confluentDataTest() {
//        NotationTestRunner.dataTest(ProtobufNotation.NOTATION_NAME, new ProtobufDataObjectMapper(new ConfluentProtobufDescriptorFileElementMapper()));
    }

    @Test
    void apicurioSerdeTest() {
        final var registryClient = new MockApicurioSchemaRegistryClient();
        final var provider = new ApicurioProtobufNotationProvider(registryClient);
        final var notationContext = new NotationContext(provider.notationName(), provider.vendorName(), registryClient.configs());
        final var notation = provider.createNotation(notationContext);
        NotationTestRunner.serdeTest(notation, true, PROTOBUF_FLAGS);
    }

    @Test
    void confluentSerdeTest() {
//        final var registryClient = new MockConfluentSchemaRegistryClient();
//        final var provider = new ConfluentProtobufNotationProvider(registryClient);
//        final var notationContext = new NotationContext(provider.notationName(), provider.vendorName(), registryClient.configs());
//        final var confluentProtobuf = provider.createNotation(notationContext);
//        NotationTestRunner.serdeTest(confluentProtobuf, true);
    }

//    @Test
//    void testSchemaMapper() {
//        // This method checks for code completeness of ProtobufSchemaMapper. It will warn if schema translation to
//        // DataSchema and back gives deltas between ProtobufSchemas.
//        final String name = "sensor_data";
//        final String schemaIn =
//                """
//                            syntax = "proto3";
//                            package io.axual.ksml.example;
//                            message sensor_data {
//                              string name = 1;
//                              int64 timestamp = 2;
//                              string value = 3;
//                              SensorType type = 4;
//                              string unit = 5;
//                              optional string color = 6;
//                              optional string city = 7;
//                              optional string owner = 8;
//                              enum SensorType {
//                                UNSPECIFIED = 0;
//                                AREA = 1;
//                                HUMIDITY = 2;
//                                LENGTH = 3;
//                                STATE = 4;
//                                TEMPERATURE = 5;
//                              }
//                            }
//                        """;
//        final var mapper = new ProtobufSchemaMapper();
//        final var proto = new io.apicurio.registry.serde.protobuf.ProtobufSchemaParser<>().parseSchema(schemaIn.getBytes(), Collections.emptyMap());
//        final var internalSchema = mapper.toDataSchema(name, proto);
//        final var out = mapper.fromDataSchema(internalSchema);
//        final var outFe = out.protoFileElement();
//        final var schemaOut = outFe.toSchema();
//        final var checker = new ProtobufCompatibilityCheckerLibrary(new ProtobufFile(schemaIn), new ProtobufFile(schemaOut));
//        final var diffs = checker.findDifferences();
//        assertTrue(diffs.isEmpty(), "PROTOBUF schema {} in/out is not compatible: " + name);
//    }
}
