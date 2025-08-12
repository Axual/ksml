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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteSerdeTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("serializer returns null for null input; otherwise single-byte array with value")
    void serializerSerializesSingleByteOrNull() {
        var serde = new ByteSerde();

        byte[] serializedNull = serde.serializer().serialize(TOPIC, null);
        assertThat(serializedNull).isNull();

        byte someValue = (byte) 0x7F;
        byte[] serializedValue = serde.serializer().serialize(TOPIC, someValue);
        assertThat(serializedValue).containsExactly(someValue);
    }

    @Test
    @DisplayName("deserializer returns null for null or empty bytes; otherwise first byte as Byte")
    void deserializerReadsFirstByteOrNull() {
        var serde = new ByteSerde();

        Byte deserializedFromNull = (Byte) serde.deserializer().deserialize(TOPIC, null);
        assertThat(deserializedFromNull).isNull();

        Byte deserializedFromEmpty = (Byte) serde.deserializer().deserialize(TOPIC, new byte[]{});
        assertThat(deserializedFromEmpty).isNull();

        byte[] multiByteArray = new byte[]{(byte) 0x12, (byte) 0x34};
        Byte deserializedFromMulti = (Byte) serde.deserializer().deserialize(TOPIC, multiByteArray);
        assertThat(deserializedFromMulti).isEqualTo((byte) 0x12);
    }
}
