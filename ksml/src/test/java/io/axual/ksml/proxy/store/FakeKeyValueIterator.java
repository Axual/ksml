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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.List;
import java.util.NoSuchElementException;

/** Kafka key-value iterator over fixed entries that records whether, and how often, it was closed. */
class FakeKeyValueIterator<K> implements KeyValueIterator<K, Object> {
    private final List<KeyValue<K, Object>> entries;
    private int position = 0;
    boolean closed = false;
    int closeCount = 0;

    FakeKeyValueIterator(List<KeyValue<K, Object>> entries) {
        this.entries = entries;
    }

    @Override
    public boolean hasNext() {
        return position < entries.size();
    }

    @Override
    public KeyValue<K, Object> next() {
        if (!hasNext()) throw new NoSuchElementException();
        return entries.get(position++);
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) throw new NoSuchElementException();
        return entries.get(position).key;
    }

    @Override
    public void close() {
        closed = true;
        closeCount++;
    }

    /** Kafka window store iterator: the key of each entry is the window start time. */
    static class Window extends FakeKeyValueIterator<Long> implements WindowStoreIterator<Object> {
        Window(List<KeyValue<Long, Object>> entries) {
            super(entries);
        }
    }
}
