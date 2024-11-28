package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import java.util.*;

public class MapUtil {
    public static <K, V> Map<K, V> arrayToMap(K[] array, V fixedValue) {
        final Map<K, V> result = new LinkedHashMap<K, V>(array.length);
        Arrays.stream(array).forEach(k -> result.put(k, fixedValue));
        return result;
    }

    public static <K, V> Map<K, V> listToMap(List<K> list, V fixedValue) {
        final Map<K, V> result = new LinkedHashMap<>(list.size());
        list.forEach(k -> result.put(k, fixedValue));
        return result;
    }

    public static <K, V> Map<V, K> invertMap(Map<K, V> map) {
        final var result = new LinkedHashMap<V, K>(map.size());
        map.forEach((k, v) -> result.put(v, k));
        return result;
    }

    public static <K, V> List<K> mapToList(Map<K, V> map) {
        final var result = new ArrayList<K>(map.size());
        map.forEach((k, v) -> result.add(k));
        return result;
    }

    public static Map<String, Object> stringKeys(Map<?, ?> map) {
        final var result = new HashMap<String, Object>();
        map.forEach((key, value) -> result.put(key != null ? key.toString() : null, value));
        return result;
    }
}
