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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class WrappedSerdeTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("WrappedSerde delegates serialize/deserialize and routes configure to both delegates")
    void wrappedSerdeDelegatesAndConfigures() {
        var lastConfigured = new AtomicReference<Map<String, ?>>();
        var configuredAsKey = new AtomicReference<Boolean>();
        var serializedNoHeaders = new AtomicReference<byte[]>();
        var serializedWithHeaders = new AtomicReference<byte[]>();
        var deserializedNoHeaders = new AtomicReference<Object>();
        var deserializedWithHeaders = new AtomicReference<Object>();
        var deserializedFromBuffer = new AtomicReference<Object>();
        var serializerClosed = new AtomicBoolean(false);
        var deserializerClosed = new AtomicBoolean(false);

        var serializer = new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                lastConfigured.set(configs);
                configuredAsKey.set(isKey);
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                var result = (data == null) ? null : data.toString().getBytes();
                serializedNoHeaders.set(result);
                return result;
            }

            @Override
            public byte[] serialize(String topic, org.apache.kafka.common.header.Headers headers, Object data) {
                var result = (data == null) ? null : data.toString().getBytes();
                serializedWithHeaders.set(result);
                return result;
            }

            @Override
            public void close() {
                serializerClosed.set(true);
            }
        };
        var deserializer = new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                lastConfigured.set(configs);
                configuredAsKey.set(isKey);
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                var s = data == null ? null : new String(data);
                deserializedNoHeaders.set(s);
                return s;
            }

            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) {
                var s = data == null ? null : new String(data);
                deserializedWithHeaders.set(s);
                return s;
            }

            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, ByteBuffer data) {
                var arr = new byte[data.remaining()];
                data.get(arr);
                var s = new String(arr);
                deserializedFromBuffer.set(s);
                return s;
            }

            @Override
            public void close() {
                deserializerClosed.set(true);
            }
        };
        var base = new Serde<>() {
            @Override
            public Serializer<Object> serializer() { return serializer; }
            @Override
            public Deserializer<Object> deserializer() { return deserializer; }
        };

        var wrapped = new WrappedSerde(base);

        var config = Map.of("a", "b");
        wrapped.configure(config, true);
        assertThat(lastConfigured.get()).isEqualTo(config);
        assertThat(configuredAsKey.get()).isTrue();

        var headers = new RecordHeaders();
        var bytes1 = wrapped.serializer().serialize(TOPIC, "x");
        var bytes2 = wrapped.serializer().serialize(TOPIC, headers, "y");
        assertThat(serializedNoHeaders.get()).isEqualTo("x".getBytes());
        assertThat(serializedWithHeaders.get()).isEqualTo("y".getBytes());
        assertThat(bytes1).isEqualTo("x".getBytes());
        assertThat(bytes2).isEqualTo("y".getBytes());

        var out1 = wrapped.deserializer().deserialize(TOPIC, "a".getBytes());
        var out2 = wrapped.deserializer().deserialize(TOPIC, headers, "b".getBytes());
        var out3 = wrapped.deserializer().deserialize(TOPIC, headers, ByteBuffer.wrap("c".getBytes()));
        assertThat(deserializedNoHeaders.get()).isEqualTo("a");
        assertThat(deserializedWithHeaders.get()).isEqualTo("b");
        assertThat(deserializedFromBuffer.get()).isEqualTo("c");
        assertThat(out1).isEqualTo("a");
        assertThat(out2).isEqualTo("b");
        assertThat(out3).isEqualTo("c");

        wrapped.serializer().close();
        wrapped.deserializer().close();
        assertThat(serializerClosed).isTrue();
        assertThat(deserializerClosed).isTrue();
    }
}
