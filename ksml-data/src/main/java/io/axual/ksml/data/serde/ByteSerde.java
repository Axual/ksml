package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A simple Serde that serializes and deserializes single Java Byte values.
 * <p>
 * Serialization: if the data is null, null is returned as byte array; otherwise a single-byte array
 * containing the Byte value is produced. Deserialization returns the first byte as a Byte object or
 * null if the input is null or empty.
 */
@Getter
public class ByteSerde implements Serde<Object> {
    private static final byte[] EMPTY = null;
    private final Serializer<Object> serializer = (topic, data) -> {
        if (data == null) return EMPTY;
        return new byte[]{(Byte) data};
    };
    private final Deserializer<Object> deserializer = (topic, data) -> data != null && data.length > 0 ? (Byte) data[0] : null;
}
