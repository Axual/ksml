package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A producer writes records with a newer version of the schema than the local {@code .proto} of KSML.
 */
@SuppressWarnings("java:S2187")
class ProtobufUnknownFieldsPipelineTest {
    // The local ProtobufEvent.proto in the schemas folder, plus a new field in each message
    private static final String NEWER_PROTO = """
            syntax = "proto3";
            package io.axual.ksml.test;
            message ProtobufEventInner {
              string a = 1;
              int32 b = 2;
            }
            message ProtobufEvent {
              string name = 1;
              ProtobufEventInner inner = 2;
              string extra = 3;
            }
            """;

    @KSMLTopic(topic = "protobuf_in", valueSerde = KSMLTopic.SerdeType.BYTES)
    TestInputTopic<String, byte[]> inputTopic;

    @KSMLTopic(topic = "protobuf_kept", valueSerde = KSMLTopic.SerdeType.BYTES)
    TestOutputTopic<String, byte[]> keptTopic;

    @KSMLTopic(topic = "protobuf_rebuilt", valueSerde = KSMLTopic.SerdeType.BYTES)
    TestOutputTopic<String, byte[]> rebuiltTopic;

    @KSMLTopologyTest(topologies = "pipelines/test-protobuf-unknown-fields.yaml", schemaDirectory = "schemas")
    void keepsNewFieldsUnlessPythonReturnsTheValue() {
        final var notation = ExecutionContext.INSTANCE.notationLibrary().get(ProtobufNotation.NOTATION_NAME);
        final var newerType = new StructType((StructSchema) notation.schemaParser().parse("test", "ProtobufEvent", NEWER_PROTO));
        try (final var serde = notation.serde(newerType, false)) {
            final var event = new NativeDataObjectMapper().toDataObject(newerType,
                    Map.of("name", "n1", "inner", Map.of("a", "x", "b", 7), "extra", "more"));
            inputTopic.pipeInput("k1", serde.serializer().serialize("protobuf_in", event));

            // filter does not change the record, so the new fields are written back
            final var kept = (DataStruct) serde.deserializer().deserialize("protobuf_kept", keptTopic.readValue());
            assertThat(kept.get("name")).isEqualTo(new DataString("n1"));
            assertThat(kept.get("extra")).isEqualTo(new DataString("more"));
            assertThat(((DataStruct) kept.get("inner")).get("b")).isEqualTo(new DataInteger(7));

            // mapValues returns a new record from Python, which never had the new fields
            final var rebuilt = (DataStruct) serde.deserializer().deserialize("protobuf_rebuilt", rebuiltTopic.readValue());
            assertThat(rebuilt.get("name")).isEqualTo(new DataString("n1"));
            assertThat(rebuilt.containsKey("extra")).isFalse();
            assertThat(((DataStruct) rebuilt.get("inner")).containsKey("b")).isFalse();
        }
    }
}
