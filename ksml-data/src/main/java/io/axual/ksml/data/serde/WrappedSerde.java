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

import lombok.Getter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class WrappedSerde implements Serde<Object> {
    private final Serializer<Object> delegateSerializer;
    private final Deserializer<Object> delegateDeserializer;
    @Getter
    private final Serializer<Object> serializer;
    @Getter
    private final Deserializer<Object> deserializer;

    public WrappedSerde(Serde<Object> delegate) {
        this.delegateSerializer = delegate.serializer();
        this.delegateDeserializer = delegate.deserializer();
        this.serializer = new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                WrappedSerde.this.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(String topic, Headers headers, Object data) {
                return delegateSerializer.serialize(topic, headers, data);
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                return delegateSerializer.serialize(topic, data);
            }

            @Override
            public void close() {
                delegateSerializer.close();
            }
        };
        this.deserializer = new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                WrappedSerde.this.configure(configs, isKey);
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                return delegateDeserializer.deserialize(topic, data);
            }

            @Override
            public Object deserialize(String topic, Headers headers, byte[] data) {
                return delegateDeserializer.deserialize(topic, headers, data);
            }

            @Override
            public Object deserialize(String topic, Headers headers, ByteBuffer data) {
                return delegateDeserializer.deserialize(topic, headers, data);
            }

            @Override
            public void close() {
                delegateDeserializer.close();
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegateSerializer.configure(configs, isKey);
        delegateDeserializer.configure(configs, isKey);
    }
}
