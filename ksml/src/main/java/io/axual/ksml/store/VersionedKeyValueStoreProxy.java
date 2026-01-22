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
 * to the underlying store. This proxy exposes store methods to Python code via @HostAccess.Export.
 * <p>
 * Note: This class implements StateStore rather than VersionedKeyValueStore because
 * VersionedRecord is a final class that cannot be extended, and we need to return
 * VersionedRecordProxy to expose its methods to Python.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class VersionedKeyValueStoreProxy<K, V> extends AbstractStateStoreProxy<VersionedKeyValueStore<K,V>> implements StateStore {

    public VersionedKeyValueStoreProxy(VersionedKeyValueStore<K, V> delegate) {
        super(delegate);
    }

    // ==================== VersionedKeyValueStore methods ====================

    @HostAccess.Export
    public long put(K key, V value, long timestamp) {
        return delegate.put(key, value, timestamp);
    }

    @HostAccess.Export
    public VersionedRecordProxy<V> delete(K key, long timestamp) {
        return wrapRecord(delegate.delete(key, timestamp));
    }

    @HostAccess.Export
    public VersionedRecordProxy<V> get(K key) {
        return wrapRecord(delegate.get(key));
    }

    @HostAccess.Export
    public VersionedRecordProxy<V> get(K key, long asOfTimestamp) {
        return wrapRecord(delegate.get(key, asOfTimestamp));
    }

    private VersionedRecordProxy<V> wrapRecord(VersionedRecord<V> record) {
        return record == null ? null : new VersionedRecordProxy<>(record);
    }
}
