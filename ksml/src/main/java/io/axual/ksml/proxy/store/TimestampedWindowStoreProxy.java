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
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

import java.util.function.LongSupplier;

/**
 * A proxy for accessing Kafka Streams TimestampedWindowStore in Python code. This proxy mediates between Python and
 * Java data types and delegates all operations to the underlying store.
 * <p>
 * Methods without {@code WithTimestamp} in their name return the same results as on a plain window store, so Python
 * code works the same on both. A write without a timestamp uses the timestamp of the record that is being processed.
 * The {@code *WithTimestamp} methods also return the timestamp.
 */
public class TimestampedWindowStoreProxy extends AbstractStateStoreProxy<TimestampedWindowStore<Object, Object>> {
    private final LongSupplier recordTimestamp;

    public TimestampedWindowStoreProxy(TimestampedWindowStore<Object, Object> delegate, LongSupplier recordTimestamp) {
        this(delegate, null, null, recordTimestamp);
    }

    /** Creates a proxy for a store with these declared key and value types; {@code null} means unknown. */
    public TimestampedWindowStoreProxy(TimestampedWindowStore<Object, Object> delegate, DataType keyType, DataType valueType, LongSupplier recordTimestamp) {
        super(delegate, keyType, valueType);
        this.recordTimestamp = recordTimestamp;
    }

    // ==================== ReadOnlyWindowStore methods ====================

    @HostAccess.Export
    public Object fetch(Object key, long time) {
        return ProxyUtil.toPython(ProxyUtil.valueOf(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(key)), time)));
    }

    /** Returns {@code {value, timestamp}} for the key in the window that starts at the given time, or {@code None}. */
    @HostAccess.Export
    public Object fetchWithTimestamp(Object key, long time) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(key)), time));
    }

    @HostAccess.Export
    public Object fetch(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(key)), timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object fetchWithTimestamp(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(key)), timeFrom, timeTo), true);
    }

    @HostAccess.Export
    public Object fetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(keyFrom)), NATIVE_MAPPER.fromPython(toKey(keyTo)), timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object fetchWithTimestamp(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetch(NATIVE_MAPPER.fromPython(toKey(keyFrom)), NATIVE_MAPPER.fromPython(toKey(keyTo)), timeFrom, timeTo), true);
    }

    @HostAccess.Export
    public Object backwardFetch(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetch(NATIVE_MAPPER.fromPython(toKey(key)), timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object backwardFetchWithTimestamp(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetch(NATIVE_MAPPER.fromPython(toKey(key)), timeFrom, timeTo), true);
    }

    @HostAccess.Export
    public Object backwardFetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetch(NATIVE_MAPPER.fromPython(toKey(keyFrom)), NATIVE_MAPPER.fromPython(toKey(keyTo)), timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object backwardFetchWithTimestamp(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetch(NATIVE_MAPPER.fromPython(toKey(keyFrom)), NATIVE_MAPPER.fromPython(toKey(keyTo)), timeFrom, timeTo), true);
    }

    @HostAccess.Export
    public Object fetchAll(long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetchAll(timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object fetchAllWithTimestamp(long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.fetchAll(timeFrom, timeTo), true);
    }

    @HostAccess.Export
    public Object backwardFetchAll(long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetchAll(timeFrom, timeTo), false);
    }

    @HostAccess.Export
    public Object backwardFetchAllWithTimestamp(long timeFrom, long timeTo) {
        return ProxyUtil.toPythonTimestamped(delegate.backwardFetchAll(timeFrom, timeTo), true);
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
    public Object backwardAll() {
        return ProxyUtil.toPythonTimestamped(delegate.backwardAll(), false);
    }

    @HostAccess.Export
    public Object backwardAllWithTimestamp() {
        return ProxyUtil.toPythonTimestamped(delegate.backwardAll(), true);
    }

    // ==================== WindowStore methods ====================

    /** Stores the value in the window that starts at the given time, with the timestamp of the record being processed. */
    @HostAccess.Export
    public void put(Object key, Object value, long windowStartTimestamp) {
        put(key, value, windowStartTimestamp, recordTimestamp.getAsLong());
    }

    @HostAccess.Export
    public void put(Object key, Object value, long windowStartTimestamp, long timestamp) {
        delegate.put(toKey(key), ValueAndTimestamp.make(toValue(value), timestamp), windowStartTimestamp);
    }
}
