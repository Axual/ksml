package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataObjectSerdeTest {
    private static final String TOPIC = "myTopic";

    @Test
    @DisplayName("serialize maps via native->DataObject then serde->native; DataNull maps to null bytes")
    void serializeThroughMappersAndNullHandling() {
        Serializer<Object> delegateSerializer = (Serializer<Object>)(Serializer<?>) new StringSerializer();
        Deserializer<Object> delegateDeserializer = (Deserializer<Object>)(Deserializer<?>) new StringDeserializer();
        DataType expected = DataString.DATATYPE;
        DataObjectMapper<Object> serdeMapper = (DataObjectMapper<Object>)(DataObjectMapper<?>) new StringDataObjectMapper();
        DataObjectMapper<Object> nativeMapper = (DataObjectMapper<Object>)(DataObjectMapper<?>) new NativeDataObjectMapper();

        var serde = new DataObjectSerde("string", delegateSerializer, delegateDeserializer, expected, serdeMapper, nativeMapper);

        byte[] bytes = serde.serialize(TOPIC, "text");
        assertThat(bytes).isEqualTo("text".getBytes());

        // With headers
        var headers = new RecordHeaders();
        byte[] bytesWithHeaders = serde.serialize(TOPIC, headers, "more");
        assertThat(bytesWithHeaders).isEqualTo("more".getBytes());

        // DataNull maps to null for delegate serializer
        byte[] nullBytes = serde.serialize(TOPIC, DataNull.INSTANCE);
        assertThat(nullBytes).isNull();

        // Also test null input mapping via native mapper (null -> DataNull.INSTANCE)
        byte[] nullFromPlainNull = serde.serialize(TOPIC, null);
        assertThat(nullFromPlainNull).isNull();
    }

    @Test
    @DisplayName("deserialize wraps delegate result via serde mapper; overloads handle headers/ByteBuffer")
    void deserializeThroughMapperForAllOverloads() {
        Serializer<Object> delegateSerializer = (Serializer<Object>)(Serializer<?>) new StringSerializer();
        Deserializer<Object> delegateDeserializer = (Deserializer<Object>)(Deserializer<?>) new StringDeserializer();
        DataType expected = DataString.DATATYPE;
        DataObjectMapper<Object> serdeMapper = (DataObjectMapper<Object>)(DataObjectMapper<?>) new StringDataObjectMapper();
        DataObjectMapper<Object> nativeMapper = (DataObjectMapper<Object>)(DataObjectMapper<?>) new NativeDataObjectMapper();

        var serde = new DataObjectSerde("string", delegateSerializer, delegateDeserializer, expected, serdeMapper, nativeMapper);

        byte[] valueBytes = new StringSerializer().serialize(TOPIC, "hello");
        Object out1 = serde.deserialize(TOPIC, valueBytes);
        assertThat(out1).isInstanceOf(DataString.class).extracting(o -> ((DataString) o).value()).isEqualTo("hello");

        var headers = new RecordHeaders();
        Object out2 = serde.deserialize(TOPIC, headers, valueBytes);
        assertThat(out2).isInstanceOf(DataString.class).extracting(o -> ((DataString) o).value()).isEqualTo("hello");

        Object out3 = serde.deserialize(TOPIC, headers, ByteBuffer.wrap(valueBytes));
        assertThat(out3).isInstanceOf(DataString.class).extracting(o -> ((DataString) o).value()).isEqualTo("hello");
    }

    @Test
    @DisplayName("exceptions are wrapped with readable messages on de/serialization")
    void exceptionsAreWrapped() {
        Serializer<Object> throwingSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) { throw new RuntimeException("boom"); }
            @Override
            public byte[] serialize(String topic, org.apache.kafka.common.header.Headers headers, Object data) { throw new RuntimeException("kaboom"); }
        };
        Deserializer<Object> throwingDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) { throw new RuntimeException("ouch"); }
            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) { throw new RuntimeException("ow"); }
            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, ByteBuffer data) { throw new RuntimeException("aiee"); }
        };
        DataObjectMapper<Object> identitySerdeMapper = new DataObjectMapper<>() {
            @Override public DataObject toDataObject(DataType expected, Object value) { return new DataString("unused"); }
            @Override public Object fromDataObject(DataObject object) { return "ignored"; }
        };
        DataObjectMapper<Object> identityNativeMapper = new DataObjectMapper<>() {
            @Override public DataObject toDataObject(Object value) { return new DataString("ignored"); }
            @Override public DataObject toDataObject(DataType expected, Object value) { return new DataString("ignored"); }
            @Override public Object fromDataObject(DataObject object) { return "ignored"; }
        };
        var serde = new DataObjectSerde("testName", throwingSerializer, throwingDeserializer, DataString.DATATYPE, identitySerdeMapper, identityNativeMapper);

        assertThatThrownBy(() -> serde.serialize(TOPIC, "x"))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("TESTNAME message could not be serialized to topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> serde.serialize(TOPIC, new RecordHeaders(), "x"))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("TESTNAME message could not be serialized to topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);

        var bytes = new StringSerializer().serialize(TOPIC, "x");
        assertThatThrownBy(() -> serde.deserialize(TOPIC, bytes))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("TESTNAME message could not be deserialized from topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> serde.deserialize(TOPIC, new RecordHeaders(), bytes))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("TESTNAME message could not be deserialized from topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> serde.deserialize(TOPIC, new RecordHeaders(), ByteBuffer.wrap(bytes)))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("TESTNAME message could not be deserialized from topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);
    }
}
