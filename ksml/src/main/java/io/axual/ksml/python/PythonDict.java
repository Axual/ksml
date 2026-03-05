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

    private final Map<String, Object> map;

    public PythonDict(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public long getHashSize() {
        return map.size();
    }

    @Override
    public boolean hasHashEntry(Value key) {
        return map.containsKey(keyFrom(key));
    }

    @Override
    public Object getHashValue(Value key) {
        return map.get(keyFrom(key));
    }

    @Override
    public void putHashEntry(Value key, Value value) {
        map.put(keyFrom(key), unwrap(value));
    }

    @Override
    public boolean removeHashEntry(Value key) {
        return map.remove(keyFrom(key)) != null;
    }

    @Override
    public Object getHashEntriesIterator() {
        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
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
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            joiner.add(formatValue(entry.getKey()) + ": " + formatValue(entry.getValue()));
        }
        return joiner.toString();
    }

    static String formatValue(Object value) {
        return switch (value) {
            case null -> "None";
            case String val -> "'" + val + "'";
            case Boolean val -> val ? "True" : "False";
            // PythonDict and PythonList have their own toString() which formats recursively
            default -> value.toString();
        };
    }

    static Object unwrap(Value value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isString()) {
            return value.asString();
        }
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isNumber()) {
            if (value.fitsInInt()) {
                return value.asInt();
            }
            if (value.fitsInLong()) {
                return value.asLong();
            }
            return value.asDouble();
        }
        return value;
    }

    static String keyFrom(Object value) {
        if (value instanceof Value val) value = unwrap(val);
        return value != null ? value.toString() : null;
    }
}
