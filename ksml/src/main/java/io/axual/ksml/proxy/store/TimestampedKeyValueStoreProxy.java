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

import org.apache.kafka.streams.state.KeyValueIterator;
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
        super(delegate);
        this.recordTimestamp = recordTimestamp;
    }

    // ==================== ReadOnlyKeyValueStore methods ====================

    @HostAccess.Export
    public Object get(Object key) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.get(NATIVE_MAPPER.fromPython(key))));
    }

    /** Returns {@code {value, timestamp}} for the key, or {@code None} when the key has no value. */
    @HostAccess.Export
    public Object getWithTimestamp(Object key) {
        return ProxyUtil.toPython(delegate.get(ProxyUtil.fromPython(key, "getWithTimestamp key")));
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
        return ProxyUtil.toPythonTimestamped(delegate.range(ProxyUtil.fromPython(from, "range from"), ProxyUtil.fromPython(to, "range to")), false);
    }

    @HostAccess.Export
    public Object rangeWithTimestamp(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.range(ProxyUtil.fromPython(from, "rangeWithTimestamp from"), ProxyUtil.fromPython(to, "rangeWithTimestamp to")), true);
    }

    @HostAccess.Export
    public Object reverseRange(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.reverseRange(ProxyUtil.fromPython(from, "reverseRange from"), ProxyUtil.fromPython(to, "reverseRange to")), false);
    }

    @HostAccess.Export
    public Object reverseRangeWithTimestamp(Object from, Object to) {
        return ProxyUtil.toPythonTimestamped(delegate.reverseRange(ProxyUtil.fromPython(from, "reverseRangeWithTimestamp from"), ProxyUtil.fromPython(to, "reverseRangeWithTimestamp to")), true);
    }

    /**
     * Entries whose key starts with the string prefix, serialized as UTF-8; an empty prefix gives all entries.
     * A prefix that is not a string raises TypeError.
     */
    @HostAccess.Export
    public Object prefixScan(Object prefix) {
        return ProxyUtil.toPythonTimestamped(prefixIterator(prefix), false);
    }

    @HostAccess.Export
    public Object prefixScanWithTimestamp(Object prefix) {
        return ProxyUtil.toPythonTimestamped(prefixIterator(prefix), true);
    }

    private KeyValueIterator<Object, ValueAndTimestamp<Object>> prefixIterator(Object prefix) {
        final var stringPrefix = ProxyUtil.toStringPrefix(prefix);
        // Kafka's in-memory and caching stores fail on an empty prefix; every key starts with ""
        if (stringPrefix.isEmpty()) return delegate.all();
        return delegate.prefixScan(stringPrefix, ProxyUtil.PREFIX_SERIALIZER);
    }

    @HostAccess.Export
    public Object approximateNumEntries() {
        return ProxyUtil.toPython(delegate.approximateNumEntries());
    }

    // ==================== KeyValueStore methods ====================

    /** Removes the key and returns its old value, without the timestamp. */
    @HostAccess.Export
    public Object delete(Object key) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.delete(NATIVE_MAPPER.fromPython(key))));
    }

    /** Stores the value with the timestamp of the record that is being processed. A value {@code None} deletes the key. */
    @HostAccess.Export
    public void put(Object key, Object value) {
        // A delete needs no timestamp, so it also works outside record processing
        if (value == null) delegate.put(DATA_OBJECT_MAPPER.toDataObject(key), null);
        else put(key, value, recordTimestamp.getAsLong());
    }

    @HostAccess.Export
    public void put(Object key, Object value, long timestamp) {
        delegate.put(DATA_OBJECT_MAPPER.toDataObject(key), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(value), ProxyUtil.checkTimestamp(timestamp, "put")));
    }

    /** Like {@link KeyValueStoreProxy#putAll}; a dict entry may have a timestamp, others get the record timestamp. */
    @HostAccess.Export
    public void putAll(Object entries) {
        delegate.putAll(ProxyUtil.toTimestampedKeyValueList(entries, recordTimestamp));
    }

    /** Stores the value with the record timestamp if the key has none, and returns the existing value. */
    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value) {
        return putIfAbsent(key, value, recordTimestamp.getAsLong());
    }

    /** Stores the value with the given timestamp if the key has none, and returns the existing value. */
    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value, long timestamp) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.putIfAbsent(DATA_OBJECT_MAPPER.toDataObject(key), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(value), ProxyUtil.checkTimestamp(timestamp, "putIfAbsent")))));
    }
}
