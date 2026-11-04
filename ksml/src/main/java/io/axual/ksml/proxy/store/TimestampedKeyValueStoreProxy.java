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

import io.axual.ksml.data.type.DataType;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

import java.util.function.LongSupplier;

/**
 * A proxy for accessing Kafka Streams TimestampedKeyValueStore in Python code. This proxy mediates between Python and
 * Java data types and delegates all operations to the underlying store.
 * <p>
 * Methods without {@code WithTimestamp} in their name return the same results as on a plain key-value store, so
 * Python code works the same on both. Writes without a timestamp use the timestamp of the record that is being
 * processed. The {@code *WithTimestamp} methods also return the timestamp.
 */
public class TimestampedKeyValueStoreProxy extends AbstractStateStoreProxy<TimestampedKeyValueStore<Object, Object>> {
    private final LongSupplier recordTimestamp;

    public TimestampedKeyValueStoreProxy(TimestampedKeyValueStore<Object, Object> delegate, LongSupplier recordTimestamp) {
        this(delegate, null, null, recordTimestamp);
    }

    /** Creates a proxy for a store with these declared key and value types; {@code null} means unknown. */
    public TimestampedKeyValueStoreProxy(TimestampedKeyValueStore<Object, Object> delegate, DataType keyType, DataType valueType, LongSupplier recordTimestamp) {
        super(delegate, keyType, valueType);
        this.recordTimestamp = recordTimestamp;
    }

    // ==================== ReadOnlyKeyValueStore methods ====================

    @HostAccess.Export
    public Object get(Object key) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.get(NATIVE_MAPPER.fromPython(toKey(key)))));
    }

    /** Returns {@code {value, timestamp}} for the key, or {@code None} when the key has no value. */
    @HostAccess.Export
    public Object getWithTimestamp(Object key) {
        return ProxyUtil.toPython(delegate.get(NATIVE_MAPPER.fromPython(toKey(key))));
    }

    @HostAccess.Export
    public Object all() {
        return ProxyUtil.toPythonTimestamped(delegate.all(), false);
    }

    @HostAccess.Export
    public Object allWithTimestamp() {
        return ProxyUtil.toPythonTimestamped(delegate.all(), true);
    }

    @HostAccess.Export
    public Object reverseAll() {
        return ProxyUtil.toPythonTimestamped(delegate.reverseAll(), false);
    }

    @HostAccess.Export
    public Object reverseAllWithTimestamp() {
        return ProxyUtil.toPythonTimestamped(delegate.reverseAll(), true);
    }

    @HostAccess.Export
    public Object range(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.range(NATIVE_MAPPER.fromPython(toKey(from)), NATIVE_MAPPER.fromPython(toKey(to))), false);
    }

    @HostAccess.Export
    public Object rangeWithTimestamp(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.range(NATIVE_MAPPER.fromPython(toKey(from)), NATIVE_MAPPER.fromPython(toKey(to))), true);
    }

    @HostAccess.Export
    public Object reverseRange(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.reverseRange(NATIVE_MAPPER.fromPython(toKey(from)), NATIVE_MAPPER.fromPython(toKey(to))), false);
    }

    @HostAccess.Export
    public Object reverseRangeWithTimestamp(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.reverseRange(NATIVE_MAPPER.fromPython(toKey(from)), NATIVE_MAPPER.fromPython(toKey(to))), true);
    }

    /** Entries whose key starts with the string prefix, serialized as UTF-8. A non-string prefix raises TypeError. */
    @HostAccess.Export
    public Object prefixScan(Object prefix) {
        return ProxyUtil.toPythonTimestamped(delegate.prefixScan(ProxyUtil.toStringPrefix(prefix), ProxyUtil.PREFIX_SERIALIZER), false);
    }

    @HostAccess.Export
    public Object prefixScanWithTimestamp(Object prefix) {
        return ProxyUtil.toPythonTimestamped(delegate.prefixScan(ProxyUtil.toStringPrefix(prefix), ProxyUtil.PREFIX_SERIALIZER), true);
    }

    @HostAccess.Export
    public Object approximateNumEntries() {
        return ProxyUtil.toPython(delegate.approximateNumEntries());
    }

    // ==================== KeyValueStore methods ====================

    /** Removes the key and returns its old value, without the timestamp. */
    @HostAccess.Export
    public Object delete(Object key) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.delete(NATIVE_MAPPER.fromPython(toKey(key)))));
    }

    /** Stores the value with the timestamp of the record that is being processed. */
    @HostAccess.Export
    public void put(Object key, Object value) {
        put(key, value, recordTimestamp.getAsLong());
    }

    @HostAccess.Export
    public void put(Object key, Object value, long timestamp) {
        delegate.put(DATA_OBJECT_MAPPER.toDataObject(toKey(key)), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(toValue(value)), ProxyUtil.checkTimestamp(timestamp, "put")));
    }

    /** Like {@link KeyValueStoreProxy#putAll}; a dict entry may have a timestamp, others get the record timestamp. */
    @HostAccess.Export
    public void putAll(Object entries) {
        delegate.putAll(ProxyUtil.toTimestampedKeyValueList(entries, recordTimestamp, key -> DATA_OBJECT_MAPPER.toDataObject(toKey(key)), value -> DATA_OBJECT_MAPPER.toDataObject(toValue(value))));
    }

    /** Stores the value with the record timestamp if the key has none, and returns the existing value. */
    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value) {
        return putIfAbsent(key, value, recordTimestamp.getAsLong());
    }

    /** Stores the value with the given timestamp if the key has none, and returns the existing value. */
    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value, long timestamp) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.putIfAbsent(DATA_OBJECT_MAPPER.toDataObject(toKey(key)), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(toValue(value)), ProxyUtil.checkTimestamp(timestamp, "putIfAbsent")))));
    }
}
