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
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * A {@link ProxyArray} implementation that renders as a Python list when
 * {@code toString()} is called. This ensures readable log output when Python
 * code logs list values via SLF4J (which calls {@code toString()} on the Java side).
 */
public class PythonList implements ProxyArray {
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    private static final ValuePrinter VALUE_PRINTER = new PythonValuePrinter();
    private final List<Object> list = new ArrayList<>();

    public PythonList(List<?> list) {
        list.forEach(element -> this.list.add(NATIVE_MAPPER.toPython(element)));
    }

    @Override
    public Object get(long index) {
        return list.get((int) index);
    }

    @Override
    public void set(long index, Value value) {
        list.set((int) index, value);
    }

    @Override
    public long getSize() {
        return list.size();
    }

    @Override
    public boolean remove(long index) {
        list.remove((int) index);
        return true;
    }

    /**
     * Return a String representation of this PythonList, in Python format.
     *
     * @return String representation of this PythonList, in Python format
     */
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        list.forEach(element -> joiner.add(VALUE_PRINTER.print(element, true)));
        return joiner.toString();
    }
}
