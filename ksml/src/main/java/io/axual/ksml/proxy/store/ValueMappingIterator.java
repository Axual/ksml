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

import java.util.function.Function;

/** A Kafka store iterator that converts the value of each entry, and passes the key and close() on unchanged. */
class ValueMappingIterator<K, A, B> implements KeyValueIterator<K, B> {
    private final KeyValueIterator<K, A> delegate;
    private final Function<A, B> mapper;

    ValueMappingIterator(KeyValueIterator<K, A> delegate, Function<A, B> mapper) {
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public KeyValue<K, B> next() {
        final var entry = delegate.next();
        return KeyValue.pair(entry.key, mapper.apply(entry.value));
    }

    @Override
    public K peekNextKey() {
        return delegate.peekNextKey();
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** The same for a window store iterator, whose keys are the window start times. */
    static final class Window<A, B> extends ValueMappingIterator<Long, A, B> implements WindowStoreIterator<B> {
        Window(WindowStoreIterator<A> delegate, Function<A, B> mapper) {
            super(delegate, mapper);
        }
    }
}
