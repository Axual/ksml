package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting Java objects to Python-compatible proxy objects
 * and vice versa. This enables proper interop with GraalVM's HostAccess.EXPLICIT
 * mode where direct access to Java collection methods is restricted.
 */
public final class PythonTypeConverter {

    private PythonTypeConverter() {
        // Utility class
    }

    /**
     * Convert a Java object to a Python-compatible proxy object.
     * Maps become ProxyHashMap, Lists become ProxyArray.
     * GraalVM Value objects are unwrapped and their contents converted.
     *
     * @param value the Java object to convert
     * @return a Python-compatible object (ProxyHashMap, ProxyArray, or the original value)
     */
    public static Object toPython(Object value) {
        if (value == null) {
            return null;
        }
        // Handle GraalVM Value objects by unwrapping them first
        if (value instanceof Value v) {
            return valueToPython(v);
        }
        if (value instanceof Map<?, ?> map) {
            return mapToPython(map);
        }
        if (value instanceof List<?> list) {
            return listToPython(list);
        }
        // Primitives, strings, and other types pass through unchanged
        return value;
    }

    /**
     * Convert a GraalVM Value to a Python-compatible object.
     * This handles Values that wrap Java Maps/Lists (e.g., from PythonDataObjectMapper).
     */
    private static Object valueToPython(Value value) {
        if (value.isNull()) {
            return null;
        }
        // Check if the Value wraps a host object (Java object)
        if (value.isHostObject()) {
            Object hostObject = value.asHostObject();
            // Recursively convert the unwrapped host object
            return toPython(hostObject);
        }
        // Handle Value with hash entries (dict-like)
        if (value.hasHashEntries()) {
            Map<Object, Object> converted = new HashMap<>();
            Value keysIterator = value.getHashKeysIterator();
            while (keysIterator.hasIteratorNextElement()) {
                Value key = keysIterator.getIteratorNextElement();
                Value val = value.getHashValue(key);
                converted.put(toPython(key), toPython(val));
            }
            return ProxyHashMap.from(converted);
        }
        // Handle Value with array elements (list-like)
        if (value.hasArrayElements()) {
            long size = value.getArraySize();
            Object[] converted = new Object[(int) size];
            for (int i = 0; i < size; i++) {
                converted[i] = toPython(value.getArrayElement(i));
            }
            return ProxyArray.fromArray(converted);
        }
        // For primitive values, return as-is (GraalVM handles these)
        return value;
    }

    /**
     * Convert a Map to a ProxyHashMap for Python interop.
     * Recursively converts nested Maps and Lists.
     */
    private static ProxyHashMap mapToPython(Map<?, ?> map) {
        Map<Object, Object> converted = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            converted.put(toPython(entry.getKey()), toPython(entry.getValue()));
        }
        return ProxyHashMap.from(converted);
    }

    /**
     * Convert a List to a ProxyArray for Python interop.
     * Recursively converts nested Maps and Lists.
     */
    private static ProxyArray listToPython(List<?> list) {
        Object[] converted = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            converted[i] = toPython(list.get(i));
        }
        return ProxyArray.fromArray(converted);
    }

    /**
     * Convert a Python Value back to a Java object.
     * Python dicts become HashMap, Python lists become ArrayList.
     *
     * @param value the GraalVM Value to convert
     * @return a Java object (HashMap, ArrayList, or the original value)
     */
    public static Object fromPython(Value value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.hasHashEntries()) {
            return hashEntriesToJava(value);
        }
        if (value.hasArrayElements()) {
            return arrayToJava(value);
        }
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isNumber()) {
            if (value.fitsInLong()) {
                return value.asLong();
            }
            if (value.fitsInDouble()) {
                return value.asDouble();
            }
        }
        if (value.isString()) {
            return value.asString();
        }
        // Return as-is for other types
        return value;
    }

    /**
     * Convert a Python dict (or any Value with hash entries) to a Java HashMap.
     */
    private static Map<Object, Object> hashEntriesToJava(Value value) {
        Map<Object, Object> result = new HashMap<>();
        Value keysIterator = value.getHashKeysIterator();
        while (keysIterator.hasIteratorNextElement()) {
            Value key = keysIterator.getIteratorNextElement();
            Value val = value.getHashValue(key);
            result.put(fromPython(key), fromPython(val));
        }
        return result;
    }

    /**
     * Convert a Python list (or any Value with array elements) to a Java ArrayList.
     */
    private static List<Object> arrayToJava(Value value) {
        List<Object> result = new ArrayList<>();
        long size = value.getArraySize();
        for (long i = 0; i < size; i++) {
            result.add(fromPython(value.getArrayElement(i)));
        }
        return result;
    }
}
