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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.graalvm.polyglot.proxy.ProxyIterator;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Python view of a Kafka store iterator. Works with Python {@code for}/{@code next()}/{@code list()}
 * and with {@code hasNext()}, {@code next()}, {@code peekNextKey()}, {@code close()}.
 * Closes the Kafka iterator when all entries are read, or when the Python function that created it returns.
 */
@Slf4j
public class StoreIteratorProxy implements AbstractProxy, ProxyIterator, ProxyObject {
    /** Iterators created by the Python function calls running on this thread. */
    private static final ThreadLocal<CallScope> SCOPE = ThreadLocal.withInitial(CallScope::new);

    private static final class CallScope {
        private final List<StoreIteratorProxy> created = new ArrayList<>();
        private int depth = 0;
    }

    private final KeyValueIterator<?, ?> iterator;
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
        this.iterator = iterator;
        final var scope = SCOPE.get();
        if (scope.depth > 0) scope.created.add(this);
    }

    /** Starts a Python function call. Returns the mark to pass to {@link #endCall(int)}. */
    public static int beginCall() {
        final var scope = SCOPE.get();
        scope.depth++;
        return scope.created.size();
    }

    /** Ends a Python function call: closes the iterators created since the mark. Returns how many were open. */
    public static int endCall(int mark) {
        final var scope = SCOPE.get();
        int closedNow = 0;
        while (scope.created.size() > mark) {
            final var proxy = scope.created.remove(scope.created.size() - 1);
            if (proxy.closed) continue;
            closedNow++;
            try {
                proxy.close();
            } catch (RuntimeException e) {
                log.warn("Could not close a store iterator", e);
            }
        }
        scope.depth--;
        if (scope.depth == 0) SCOPE.remove();
        return closedNow;
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
        return ProxyUtil.toPython(iterator.next());
    }

    /** Java style next: returns the next entry, or {@code null} when there is none. */
    public Object next() {
        return hasNext() ? getNext() : null;
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
