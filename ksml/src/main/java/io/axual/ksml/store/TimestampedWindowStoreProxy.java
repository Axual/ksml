package io.axual.ksml.store;

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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.HostAccess;

import java.time.Instant;

/**
 * A proxy wrapper around a Kafka Streams TimestampedWindowStore that delegates all operations
 * to the underlying store. This proxy exposes store methods to Python code via @HostAccess.Export.
 * <p>
 * The value type is ValueAndTimestamp, and methods that return values wrap them in
 * ValueAndTimestampProxy for Python accessibility.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class TimestampedWindowStoreProxy<K, V> extends AbstractStateStoreProxy<TimestampedWindowStore<K, V>> {

    public TimestampedWindowStoreProxy(TimestampedWindowStore<K, V> delegate) {
        super(delegate);
    }

    // ==================== WindowStore methods ====================

    @HostAccess.Export
    public void put(K key, ValueAndTimestamp<V> value, long windowStartTimestamp) {
        delegate.put(key, value, windowStartTimestamp);
    }

    @HostAccess.Export
    public WindowStoreIterator<ValueAndTimestampProxy<V>> fetch(K key, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetch(K, long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<ValueAndTimestampProxy<V>> fetch(K key, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetch(K, Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<ValueAndTimestampProxy<V>> backwardFetch(K key, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<ValueAndTimestampProxy<V>> backwardFetch(K key, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> fetch(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetch(K, K, long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> fetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetch(K, K, Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> backwardFetch(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K, long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> backwardFetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K, Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> fetchAll(long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetchAll(long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> fetchAll(Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetchAll(Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> backwardFetchAll(long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetchAll(long, long) is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> backwardFetchAll(Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetchAll(Instant, Instant) is not supported by this proxy (" + getClass().getName() + ")");
    }

    // ==================== ReadOnlyWindowStore methods ====================

    @HostAccess.Export
    public ValueAndTimestampProxy<V> fetch(K key, long time) {
        return wrap(delegate.fetch(key, time));
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> all() {
        throw new UnsupportedOperationException("all() is not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, ValueAndTimestampProxy<V>> backwardAll() {
        throw new UnsupportedOperationException("backwardAll() is not supported by this proxy (" + getClass().getName() + ")");
    }

    /**
     * Wrap the returned value in a proxy which is accessible from Python.
     *
     * @param valueAndTimestamp the value to wrap
     * @return the wrapped value, or null if the input is null
     */
    private ValueAndTimestampProxy<V> wrap(ValueAndTimestamp<V> valueAndTimestamp) {
        return valueAndTimestamp == null ? null : new ValueAndTimestampProxy<>(valueAndTimestamp);
    }
}
