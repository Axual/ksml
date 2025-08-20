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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderFilterSerdeTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("HeaderFilterSerde removes filtered headers on serialize and deserializes with filtered copies")
    void headerFilteringOnSerializeAndDeserialize() {
        var seenSerializeHeaders = new AtomicReference<Iterable<Header>>();
        var seenDeserializeHeaders = new AtomicReference<Iterable<Header>>();

        var baseSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, org.apache.kafka.common.header.Headers headers, Object data) {
                seenSerializeHeaders.set(headers);
                return data == null ? null : data.toString().getBytes();
            }
            @Override
            public byte[] serialize(String topic, Object data) { return null; }
        };
        var baseDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) {
                seenDeserializeHeaders.set(headers);
                return data == null ? null : new String(data);
            }
            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, ByteBuffer data) {
                seenDeserializeHeaders.set(headers);
                var arr = new byte[data.remaining()];
                data.get(arr);
                return new String(arr);
            }
            @Override
            public Object deserialize(String topic, byte[] data) { return null; }
        };
        var base = new Serde<>() {
            @Override public Serializer<Object> serializer() { return baseSerializer; }
            @Override public Deserializer<Object> deserializer() { return baseDeserializer; }
        };

        var serde = new HeaderFilterSerde(base);
        serde.filteredHeaders = Set.of("keep", "drop");

        var headers = new RecordHeaders(new Header[]{
                new RecordHeader("keep", "1".getBytes()),
                new RecordHeader("drop", "2".getBytes()),
                new RecordHeader("other", "3".getBytes())
        });

        // Serialize with headers and non-null value: afterwards headers should lose filtered keys
        var result = serde.serializer().serialize(TOPIC, headers, "value");
        assertThat(result).isEqualTo("value".getBytes());
        var remainingKeysAfterSerialize = headers.toArray();
        assertThat(remainingKeysAfterSerialize)
                .extracting(Header::key)
                .containsExactly("other");

        // Deserialize with headers: delegate must see a copy without filtered keys, original remains intact
        var headersForDeserialize = new RecordHeaders(new Header[]{
                new RecordHeader("keep", "a".getBytes()),
                new RecordHeader("drop", "b".getBytes()),
                new RecordHeader("other", "c".getBytes())
        });
        var out1 = serde.deserializer().deserialize(TOPIC, headersForDeserialize, "x".getBytes());
        assertThat(out1).isEqualTo("x");
        assertThat(seenDeserializeHeaders.get()).extracting(Header::key).containsExactly("other");
        // original headers still have all
        assertThat(headersForDeserialize.toArray()).extracting(Header::key).containsExactly("keep", "drop", "other");

        var out2 = serde.deserializer().deserialize(TOPIC, headersForDeserialize, ByteBuffer.wrap("y".getBytes()));
        assertThat(out2).isEqualTo("y");
        assertThat(seenDeserializeHeaders.get()).extracting(Header::key).containsExactly("other");

        // Also test serialize(null data) path which should still remove filtered headers
        var headersWhenNull = new RecordHeaders(new Header[]{
                new RecordHeader("keep", "k".getBytes()),
                new RecordHeader("drop", "d".getBytes()),
                new RecordHeader("other", "o".getBytes())
        });
        var resultNull = serde.serializer().serialize(TOPIC, headersWhenNull, null);
        assertThat(resultNull).isNull();
        assertThat(headersWhenNull.toArray()).extracting(Header::key).containsExactly("other");
    }

    @Test
    @DisplayName("configure calls are delegated to underlying serializer and deserializer")
    void configureDelegates() {
        var seenConfig = new AtomicReference<Map<String, ?>>();
        var baseSerializer = new Serializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) { seenConfig.set(configs); }
            @Override public byte[] serialize(String topic, Object data) { return null; }
        };
        var baseDeserializer = new Deserializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) { seenConfig.set(configs); }
            @Override public Object deserialize(String topic, byte[] data) { return null; }
        };
        var serde = new HeaderFilterSerde(new Serde<>() {
            @Override public Serializer<Object> serializer() { return baseSerializer; }
            @Override public Deserializer<Object> deserializer() { return baseDeserializer; }
        });

        var config = Map.of("x", 1);
        serde.configure(config, false);
        assertThat(seenConfig.get()).isEqualTo(config);
    }
}
