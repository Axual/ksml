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

import io.axual.ksml.data.util.ValuePrinter;
import io.axual.ksml.data.value.Struct;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyHashMap;
import org.graalvm.polyglot.proxy.ProxyIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A {@link ProxyHashMap} implementation that renders as a Python dict when
 * {@code toString()} is called. This ensures readable log output when Python
 * code logs map values via SLF4J (which calls {@code toString()} on the Java side).
 */
public class PythonDict implements ProxyHashMap {
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    private static final ValuePrinter VALUE_PRINTER = new PythonValuePrinter();
    private final Struct<Object> struct = new Struct<>();

    public PythonDict(Map<?, ?> map) {
        map.forEach((k, v) -> struct.put(keyFrom(k), NATIVE_MAPPER.toPython(v)));
    }

    @Override
    public long getHashSize() {
        return struct.size();
    }

    @Override
    public boolean hasHashEntry(Value key) {
        return struct.containsKey(keyFrom(key));
    }

    @Override
    public Object getHashValue(Value key) {
        return struct.get(keyFrom(key));
    }

    @Override
    public void putHashEntry(Value key, Value value) {
        struct.put(keyFrom(key), value);
    }

    @Override
    public boolean removeHashEntry(Value key) {
        return struct.remove(keyFrom(key)) != null;
    }

    @Override
    public Object getHashEntriesIterator() {
        Iterator<Map.Entry<String, Object>> it = struct.entrySet().iterator();
        return new ProxyIterator() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Object getNext() {
                Map.Entry<String, Object> entry = it.next();
                return ProxyArray.fromArray(entry.getKey(), entry.getValue());
            }
        };
    }

    /**
     * Return a String representation of this PythonDict, in Python format.
     *
     * @return String representation of this PythonDict, in Python format
     */
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
        struct.forEach((k, v) -> joiner.add(VALUE_PRINTER.print(k, true) + ": " + VALUE_PRINTER.print(v, true)));
        return joiner.toString();
    }

    private String keyFrom(Object key) {
        if (key == null) return null;
        if (key instanceof Value value && value.isString()) return value.asString();
        return key.toString();
    }
}
