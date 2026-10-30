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

import io.axual.ksml.data.object.DataStruct;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TombstoneSerdeTest {
    private static final String TOPIC = "topic";

    // Like a vendor notation: null bytes become a typed null object, and null is written as a value
    private static final Serde<Object> NOTATION_SERDE = new Serde<>() {
        @Override
        public Serializer<Object> serializer() {
            return (topic, data) -> String.valueOf(data).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Deserializer<Object> deserializer() {
            return (topic, data) -> data != null ? data.length : new DataStruct(null, true);
        }
    };

    private final Serde<Object> serde = new TombstoneSerde(NOTATION_SERDE);

    @Test
    @DisplayName("A tombstone is deserialized as null, also when the notation makes a typed null of it")
    void tombstoneIsNull() {
        assertThat(serde.deserializer().deserialize(TOPIC, null)).isNull();
        assertThat(serde.deserializer().deserialize(TOPIC, new RecordHeaders(), (byte[]) null)).isNull();
        assertThat(serde.deserializer().deserialize(TOPIC, new RecordHeaders(), (ByteBuffer) null)).isNull();
    }

    @Test
    @DisplayName("Other data, empty bytes included, is deserialized by the notation")
    void otherDataGoesToTheNotation() {
        assertThat(serde.deserializer().deserialize(TOPIC, new byte[]{1, 2})).isEqualTo(2);
        assertThat(serde.deserializer().deserialize(TOPIC, new byte[0])).isEqualTo(0);
        assertThat(serde.deserializer().deserialize(TOPIC, new RecordHeaders(), new byte[]{1})).isEqualTo(1);
    }

    @Test
    @DisplayName("null is serialized as a tombstone, also when the notation would write a value")
    void nullIsTombstone() {
        assertThat(serde.serializer().serialize(TOPIC, null)).isNull();
        assertThat(serde.serializer().serialize(TOPIC, new RecordHeaders(), null)).isNull();
    }

    @Test
    @DisplayName("Other data is serialized by the notation")
    void otherDataGoesToTheNotationSerializer() {
        assertThat(serde.serializer().serialize(TOPIC, "ab")).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));
        assertThat(serde.serializer().serialize(TOPIC, new RecordHeaders(), "ab")).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));
    }
}
