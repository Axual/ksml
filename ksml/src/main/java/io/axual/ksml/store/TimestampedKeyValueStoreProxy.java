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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

import java.util.List;

/**
 * A proxy wrapper around a Kafka Streams TimestampedKeyValueStore that delegates all operations
 * to the underlying store. This proxy exposes store methods to Python code via @HostAccess.Export.
 * <p>
 * Note: This class implements StateStore rather than VersionedKeyValueStore because
 * VersionedRecord is a final class that cannot be extended, and we need to return
 * VersionedRecordProxy to expose its methods to Python.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class TimestampedKeyValueStoreProxy<K, V> extends AbstractStateStoreProxy<TimestampedKeyValueStore<K,V>>  {

    @HostAccess.Export
    public TimestampedKeyValueStoreProxy(TimestampedKeyValueStore<K, V> delegate) {
        super(delegate);
    }

    @HostAccess.Export
    public void put(K key, ValueAndTimestampProxy<V> value) {
        delegate.put(key, value.delegate());
    }

    @HostAccess.Export
    public void put(K key, V value, long timestamp) {
        delegate.put(key, ValueAndTimestamp.make(value, timestamp));
    }

    @HostAccess.Export
    public Object putIfAbsent(K key, ValueAndTimestampProxy<V> value) {
        return toPython(delegate.putIfAbsent(key, value.delegate()));
    }

    @HostAccess.Export
    public Object putIfAbsent(K key, V value, long timestamp) {
        return toPython(delegate.putIfAbsent(key, ValueAndTimestamp.make(value, timestamp)));
    }

    @HostAccess.Export
    public void putAll(List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        throw new UnsupportedOperationException("putAll(List<KeyValue<K, ValueAndTimestamp<V>>>) not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public Object delete(K key) {
        return toPython(delegate.delete(key));
    }

    @HostAccess.Export
    public Object get(K key) {
        return toPython(delegate.get(key));
    }

    @HostAccess.Export
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(K from, K to) {
        throw new UnsupportedOperationException("range(from, to) not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        throw new UnsupportedOperationException("all() not supported by this proxy (" + getClass().getName() + ")");
    }

    @HostAccess.Export
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }
}
