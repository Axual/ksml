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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy wrapper around a Kafka Streams VersionedKeyValueStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class VersionedKeyValueStoreProxy<K, V> implements VersionedKeyValueStore<K, V> {
    private final VersionedKeyValueStore<K, V> delegate;

    public VersionedKeyValueStoreProxy(VersionedKeyValueStore<K, V> delegate) {
        this.delegate = delegate;
    }

    // ==================== VersionedKeyValueStore methods ====================

    @Override
    @HostAccess.Export
    public long put(K key, V value, long timestamp) {
        return delegate.put(key, value, timestamp);
    }

    @Override
    @HostAccess.Export
    public VersionedRecord<V> delete(K key, long timestamp) {
        return delegate.delete(key, timestamp);
    }

    @Override
    @HostAccess.Export
    public VersionedRecord<V> get(K key) {
        return delegate.get(key);
    }

    @Override
    @HostAccess.Export
    public VersionedRecord<V> get(K key, long asOfTimestamp) {
        return delegate.get(key, asOfTimestamp);
    }

    // ==================== StateStore methods ====================

    @Override
    @HostAccess.Export
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        delegate.init(context, root);
    }

    @Override
    @HostAccess.Export
    public void flush() {
        delegate.flush();
    }

    @Override
    @HostAccess.Export
    public void close() {
        delegate.close();
    }

    @Override
    @HostAccess.Export
    public boolean persistent() {
        return delegate.persistent();
    }

    @Override
    @HostAccess.Export
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    @HostAccess.Export
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return delegate.query(query, positionBound, config);
    }

    @Override
    @HostAccess.Export
    public Position getPosition() {
        return delegate.getPosition();
    }
}
