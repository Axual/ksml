package io.axual.ksml.proxy.store;

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

import io.axual.ksml.proxy.base.AbstractProxy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.graalvm.polyglot.proxy.ProxyIterator;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Wraps a Kafka Streams store iterator ({@link KeyValueIterator}, which includes
 * {@link org.apache.kafka.streams.state.WindowStoreIterator}) for Python.
 * <p>
 * As a {@link ProxyIterator}, it works with Python {@code for}, {@code next()} and {@code list()}. As a
 * {@link ProxyObject}, it keeps the Java style methods {@code hasNext()}, {@code next()}, {@code peekNextKey()}
 * and {@code close()}. Both work with {@code HostAccess.EXPLICIT}, because polyglot proxies are not host objects.
 * <p>
 * The Kafka iterator is closed once it is used up, and by {@code close()}, so a Python loop that reads all entries
 * does not leak it. For an early exit, Python code uses {@code contextlib.closing(...)}.
 */
public class StoreIteratorProxy implements AbstractProxy, ProxyIterator, ProxyObject {
    private final KeyValueIterator<?, ?> iterator;
    private final Function<KeyValue<?, ?>, Object> entryToPython;
    private final Map<String, ProxyExecutable> members = Map.of(
            "hasNext", args -> hasNext(),
            "next", args -> next(),
            "peekNextKey", args -> peekNextKey(),
            "close", args -> {
                close();
                return null;
            });
    private boolean closed = false;

    public StoreIteratorProxy(KeyValueIterator<?, ?> iterator) {
        this(iterator, ProxyUtil::toPython);
    }

    /** Creates an iterator proxy that converts each Kafka entry for Python with the given function. */
    public StoreIteratorProxy(KeyValueIterator<?, ?> iterator, Function<KeyValue<?, ?>, Object> entryToPython) {
        this.iterator = iterator;
        this.entryToPython = entryToPython;
    }

    /** Returns whether there is a next entry, and closes the Kafka iterator when there is none. */
    @Override
    public boolean hasNext() {
        if (closed) return false;
        if (iterator.hasNext()) return true;
        close();
        return false;
    }

    /** Used by Python iteration: a {@link NoSuchElementException} at the end becomes {@code StopIteration}. */
    @Override
    public Object getNext() {
        if (!hasNext()) throw new NoSuchElementException();
        return entryToPython.apply(iterator.next());
    }

    /** Java style next: returns the next entry, or {@code null} when there is none. */
    public Object next() {
        if (!hasNext()) return null;
        return entryToPython.apply(iterator.next());
    }

    /** Returns the key of the next entry without moving forward, or {@code null} when there is none. */
    public Object peekNextKey() {
        if (!hasNext()) return null;
        return ProxyUtil.toPython(iterator.peekNextKey());
    }

    public void close() {
        if (!closed) iterator.close();
        closed = true;
    }

    @Override
    public Object getMember(String key) {
        return members.get(key);
    }

    @Override
    public Object getMemberKeys() {
        return ProxyArray.fromArray(members.keySet().toArray());
    }

    @Override
    public boolean hasMember(String key) {
        return members.containsKey(key);
    }

    @Override
    public void putMember(String key, Value value) {
        throw new UnsupportedOperationException("Store iterators are read-only");
    }
}
