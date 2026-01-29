package io.axual.ksml.python;

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

import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataPrimitive;
import io.axual.ksml.data.object.DataStruct;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;
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

            // Handle KSML DataObject types
            case DataNull ignored -> null;
            case DataPrimitive<?> primitive -> primitive.value();
            case DataStruct struct -> dataStructToPython(struct);
            case DataMap dataMap -> dataMapToPython(dataMap);
            case DataList dataList -> dataListToPython(dataList);

            // Handle Kafka Streams timestamped/versioned record types
            case ValueAndTimestamp<?> vat -> valueAndTimestampToPython(vat);
            case VersionedRecord<?> vr -> versionedRecordToPython(vr);

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
     * Convert a DataStruct to a ProxyHashMap for Python interop.
     * Recursively converts nested DataObject values.
     */
    private static Object dataStructToPython(DataStruct struct) {
        if (struct.isNull()) {
            return null;
        }
        Map<Object, Object> converted = new HashMap<>();
        for (var entry : struct.entrySet()) {
            converted.put(entry.getKey(), toPython(entry.getValue()));
        }
        return ProxyHashMap.from(converted);
    }

    /**
     * Convert a DataMap to a ProxyHashMap for Python interop.
     * Recursively converts nested DataObject values.
     */
    private static ProxyHashMap dataMapToPython(DataMap dataMap) {
        Map<Object, Object> converted = new HashMap<>();
        for (var entry : dataMap.entrySet()) {
            converted.put(entry.getKey(), toPython(entry.getValue()));
        }
        return ProxyHashMap.from(converted);
    }

    /**
     * Convert a DataList to a ProxyArray for Python interop.
     * Recursively converts nested DataObject values.
     */
    private static ProxyArray dataListToPython(DataList dataList) {
        Object[] converted = new Object[dataList.size()];
        for (int i = 0; i < dataList.size(); i++) {
            converted[i] = toPython(dataList.get(i));
        }
        return ProxyArray.fromArray(converted);
    }

    /**
     * Convert a Kafka ValueAndTimestamp to a ProxyHashMap for Python interop.
     * Creates a dict with "value" and "timestamp" keys.
     */
    private static ProxyHashMap valueAndTimestampToPython(ValueAndTimestamp<?> vat) {
        Map<Object, Object> converted = new HashMap<>();
        converted.put("value", toPython(vat.value()));
        converted.put("timestamp", vat.timestamp());
        return ProxyHashMap.from(converted);
    }

    /**
     * Convert a Kafka VersionedRecord to a ProxyHashMap for Python interop.
     * Creates a dict with "value" and "timestamp" keys.
     */
    private static ProxyHashMap versionedRecordToPython(VersionedRecord<?> vr) {
        Map<Object, Object> converted = new HashMap<>();
        converted.put("value", toPython(vr.value()));
        converted.put("timestamp", vr.timestamp());
        return ProxyHashMap.from(converted);
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
}
