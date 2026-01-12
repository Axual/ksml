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
 * <p>
 * With HostAccess.EXPLICIT, Python cannot access methods on Java collections like
 * HashMap or ArrayList. This converter transforms them into GraalVM Proxy objects
 * (ProxyHashMap, ProxyArray) which use the polyglot proxy protocol that bypasses
 * HostAccess restrictions.
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
        return switch (value) {
            case null -> null;

            // Handle GraalVM Value objects by unwrapping them first
            case Value v -> valueToPython(v);
            case Map<?, ?> map -> mapToPython(map);
            case List<?> list -> listToPython(list);
            default ->
                // Primitives, strings, and other types pass through unchanged
                value;
        };
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
     * Convert a GraalVM Value to a Python-compatible object.
     * Host objects (Java objects wrapped in Value) are unwrapped and converted.
     * Native polyglot Values with hash/array entries are converted to Java collections first,
     * then to proxy objects.
     */
    private static Object valueToPython(Value value) {
        if (value.isNull()) {
            return null;
        }
        // Check if the Value wraps a host object (Java object) - most common case
        if (value.isHostObject()) {
            // Unwrap and recursively convert (will route to mapToPython/listToPython)
            return toPython(value.asHostObject());
        }
        // Handle native polyglot Value with hash entries (e.g., Python dict)
        if (value.hasHashEntries()) {
            // Extract to Java Map, then convert to ProxyHashMap
            return mapToPython(valueToMap(value));
        }
        // Handle native polyglot Value with array elements (e.g., Python list)
        if (value.hasArrayElements()) {
            // Extract to Java List, then convert to ProxyArray
            return listToPython(valueToList(value));
        }
        // For primitive values, return as-is (GraalVM handles these)
        return value;
    }

    /**
     * Convert a Map to a ProxyHashMap for Python interop.
     * Recursively converts nested Maps, Lists, and Values.
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
     * Recursively converts nested Maps, Lists, and Values.
     */
    private static ProxyArray listToPython(List<?> list) {
        Object[] converted = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            converted[i] = toPython(list.get(i));
        }
        return ProxyArray.fromArray(converted);
    }

    /**
     * Extract a Value with hash entries to a Java Map (without recursive conversion).
     * The actual conversion to Python types happens in mapToPython().
     */
    private static Map<Object, Object> valueToMap(Value value) {
        Map<Object, Object> result = new HashMap<>();
        Value keysIterator = value.getHashKeysIterator();
        while (keysIterator.hasIteratorNextElement()) {
            Value key = keysIterator.getIteratorNextElement();
            Value val = value.getHashValue(key);
            // Store Values as-is; toPython() in mapToPython() will handle conversion
            result.put(key, val);
        }
        return result;
    }

    /**
     * Extract a Value with array elements to a Java List (without recursive conversion).
     * The actual conversion to Python types happens in listToPython().
     */
    private static List<Object> valueToList(Value value) {
        List<Object> result = new ArrayList<>();
        long size = value.getArraySize();
        for (long i = 0; i < size; i++) {
            // Store Values as-is; toPython() in listToPython() will handle conversion
            result.add(value.getArrayElement(i));
        }
        return result;
    }


    /**
     * Convert a Value with hash entries to a Java HashMap.
     * Recursively converts nested structures.
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
     * Convert a Value with array elements to a Java ArrayList.
     * Recursively converts nested structures.
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
