package io.axual.ksml.data.value;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

public class Struct<T> extends TreeMap<String, T> {
    /**
     * Character used for identifying "meta" keys. Meta keys (keys that start with this character)
     * have a lower priority compared to standard keys when sorted.
     */
    public static final String META_ATTRIBUTE_CHAR = "@";

    /**
     * Custom comparator for sorting key-value pairs in the data structure.
     * <p>
     * The sorting logic prioritizes standard keys over "meta" keys, and within each group,
     * sorts lexicographically.
     */
    private static final Comparator<String> DEFAULT_COMPARATOR = (o1, o2) -> {
        if ((o1 == null || o1.isEmpty()) && (o2 == null || o2.isEmpty())) return 0;
        if (o1 == null || o1.isEmpty()) return -1;
        if (o2 == null || o2.isEmpty()) return 1;

        final var meta1 = o1.startsWith(META_ATTRIBUTE_CHAR);
        final var meta2 = o2.startsWith(META_ATTRIBUTE_CHAR);

        // If only the first string starts with the meta char, then sort it last
        if (meta1 && !meta2) return 1;
        // If only the second string starts with the meta char, then sort it first
        if (!meta1 && meta2) return -1;
        // If both (do not) start with the meta char, then sort as normal
        return o1.compareTo(o2);
    };

    public Struct() {
        this(null);
    }

    public Struct(Map<String, T> map) {
        super(DEFAULT_COMPARATOR);
        if (map != null) putAll(map);
    }

    public <S> Struct(Map<String, S> map, Function<S, T> mapper) {
        super(DEFAULT_COMPARATOR);
        if (map != null) map.forEach((k, v) -> put(k, mapper.apply(v)));
    }
}
