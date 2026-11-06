package io.axual.ksml.data.serde;

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

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Makes a tombstone null in Kafka Streams and null bytes on a topic, for every notation.
 * An empty value (DataNull) is written as empty bytes.
 */
public class TombstoneSerde implements Serde<Object> {
    private static final byte[] EMPTY = new byte[0];
    private final Serde<Object> serde;

    public TombstoneSerde(Serde<Object> serde) {
        this.serde = serde;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serde.configure(configs, isKey);
    }

    @Override
    public void close() {
        serde.close();
    }

    /**
     * Returns true if the data is written as a tombstone: null or a typed null, but not DataNull (an empty value).
     */
    public static boolean isTombstone(Object data) {
        return data != DataNull.INSTANCE && DataObject.isNone(data);
    }

    @Override
    public Serializer<Object> serializer() {
        return new TombstoneSerializer(serde.serializer());
    }

    @Override
    public Deserializer<Object> deserializer() {
        return new TombstoneDeserializer(serde.deserializer());
    }

    private record TombstoneSerializer(Serializer<Object> serializer) implements Serializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(String topic, Object data) {
            if (data == DataNull.INSTANCE) return EMPTY;
            return isTombstone(data) ? null : serializer.serialize(topic, data);
        }

        @Override
        public byte[] serialize(String topic, Headers headers, Object data) {
            if (data == DataNull.INSTANCE) return EMPTY;
            return isTombstone(data) ? null : serializer.serialize(topic, headers, data);
        }

        @Override
        public void close() {
            serializer.close();
        }
    }

    private record TombstoneDeserializer(Deserializer<Object> deserializer) implements Deserializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
        }

        @Override
        public Object deserialize(String topic, byte[] data) {
            return data != null ? deserializer.deserialize(topic, data) : null;
        }

        @Override
        public Object deserialize(String topic, Headers headers, byte[] data) {
            return data != null ? deserializer.deserialize(topic, headers, data) : null;
        }

        @Override
        public Object deserialize(String topic, Headers headers, ByteBuffer data) {
            return data != null ? deserializer.deserialize(topic, headers, data) : null;
        }

        @Override
        public void close() {
            deserializer.close();
        }
    }
}
