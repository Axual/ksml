package io.axual.ksml.parity;

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

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Wire format of a key or value on a topic. Both sides of a parity case read and write raw bytes, so the
 * same bytes go in, and the bytes that come out are decoded the same way.
 */
public enum Format {
    STRING, JSON, LONG;

    byte[] encode(String text) {
        if (text == null) return null;
        return switch (this) {
            case STRING, JSON -> text.getBytes(UTF_8);
            case LONG -> new LongSerializer().serialize(null, Long.parseLong(text));
        };
    }

    Object decode(byte[] bytes) {
        if (bytes == null) return null;
        return switch (this) {
            case STRING -> new String(bytes, UTF_8);
            case JSON -> Normalizer.parseJson(new String(bytes, UTF_8));
            case LONG -> bytes.length == Long.BYTES
                    ? Normalizer.normalize(new LongDeserializer().deserialize(null, bytes))
                    : "<" + bytes.length + " bytes, not a long: " + new String(bytes, UTF_8) + ">";
        };
    }

    String raw(byte[] bytes) {
        if (bytes == null) return null;
        return this == LONG && bytes.length == Long.BYTES
                ? String.valueOf(new LongDeserializer().deserialize(null, bytes))
                : new String(bytes, UTF_8);
    }
}
