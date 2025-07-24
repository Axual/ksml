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

import io.apicurio.registry.rules.compatibility.protobuf.ProtobufCompatibilityCheckerLibrary;
import io.apicurio.registry.utils.protobuf.schema.ProtobufFile;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.apicurio.MockApicurioSchemaRegistryClient;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import io.axual.ksml.data.notation.protobuf.*;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufTests {
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
        NotationTestRunner.dataTest(ProtobufNotation.NOTATION_NAME, new ProtobufDataObjectMapper(new ApicurioProtobufDescriptorFileElementMapper()));
    }

    @Test
    void confluentDataTest() {
        NotationTestRunner.dataTest(ProtobufNotation.NOTATION_NAME, new ProtobufDataObjectMapper(new ConfluentProtobufDescriptorFileElementMapper()));
    }

    void serdeTest(ProtobufSerdeProvider serdeProvider, ProtobufSchemaParser schemaParser, ProtobufDataObjectMapper protobufMapper, Map<String, String> srConfigs) {
        final var notation = new ProtobufNotation(serdeProvider, schemaParser, protobufMapper, new NativeDataObjectMapper(), srConfigs);
        NotationTestRunner.serdeTest(AvroNotation.NOTATION_NAME, notation, true);
    }

    @Test
    void apicurioSerdeTest() {
        final var registryClient = new MockApicurioSchemaRegistryClient();
        final var serdeProvider = new ApicurioProtobufSerdeProvider(registryClient);
        final var schemaParser = new ApicurioProtobufSchemaParser();
        final var protobufMapper = new ProtobufDataObjectMapper(new ApicurioProtobufDescriptorFileElementMapper());
        serdeTest(serdeProvider, schemaParser, protobufMapper, registryClient.configs());
    }

    @Test
    void confluentSerdeTest() {
        final var registryClient = new MockConfluentSchemaRegistryClient();
        final var serdeProvider = new ConfluentProtobufSerdeProvider(registryClient);
        final var schemaParser = new ConfluentProtobufSchemaParser();
        final var protobufMapper = new ProtobufDataObjectMapper(new ConfluentProtobufDescriptorFileElementMapper());
        serdeTest(serdeProvider, schemaParser, protobufMapper, registryClient.configs());
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
