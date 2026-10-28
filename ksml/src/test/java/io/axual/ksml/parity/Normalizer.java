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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Turns values from either side into plain, comparable Java values. Numbers become {@link BigDecimal} without
 * trailing zeros, so {@code 1}, {@code 1L} and {@code 1.0} compare as equal. Maps are sorted by key.
 */
final class Normalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();

    private Normalizer() {
    }

    static Object parseJson(String text) {
        try {
            final var parsed = MAPPER.readValue(text, Object.class);
            // A tombstone (no bytes) and the JSON text "null" are not the same thing on a topic
            if (parsed == null) return "<json text null>";
            return normalize(parsed);
        } catch (Exception e) {
            return "<not json: " + text + ">";
        }
    }

    static Object normalize(Object value) {
        if (value == null) return null;
        if (value instanceof DataObject dataObject) return normalize(NATIVE_MAPPER.fromDataObject(dataObject));
        if (value instanceof ValueAndTimestamp<?> vat) return normalize(vat.value());
        if (value instanceof Windowed<?> windowed)
            return normalize(windowed.key()) + "@" + windowed.window().start() + "/" + windowed.window().end();
        if (value instanceof BigDecimal bd) return strip(bd);
        if (value instanceof BigInteger bi) return strip(new BigDecimal(bi));
        if (value instanceof Double || value instanceof Float) return strip(BigDecimal.valueOf(((Number) value).doubleValue()));
        if (value instanceof Number number) return strip(BigDecimal.valueOf(number.longValue()));
        if (value instanceof CharSequence cs) return cs.toString();
        if (value instanceof byte[] bytes) return new String(bytes);
        if (value instanceof Map<?, ?> map) {
            final var result = new TreeMap<String, Object>();
            map.forEach((k, v) -> result.put(String.valueOf(normalize(k)), normalize(v)));
            return result;
        }
        if (value instanceof Collection<?> collection) {
            final var result = new ArrayList<>();
            collection.forEach(v -> result.add(normalize(v)));
            return result;
        }
        return value;
    }

    private static BigDecimal strip(BigDecimal value) {
        return value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
    }

    static String render(Object value) {
        if (value == null) return "null";
        if (value instanceof BigDecimal bd) return bd.toPlainString();
        if (value instanceof String s) return "\"" + s + "\"";
        if (value instanceof Map<?, ?> map) {
            final var sb = new StringBuilder("{");
            map.forEach((k, v) -> sb.append(sb.length() > 1 ? "," : "").append(k).append(":").append(render(v)));
            return sb.append("}").toString();
        }
        if (value instanceof Collection<?> collection) {
            final var sb = new StringBuilder("[");
            collection.forEach(v -> sb.append(sb.length() > 1 ? "," : "").append(render(v)));
            return sb.append("]").toString();
        }
        return String.valueOf(value);
    }
}
