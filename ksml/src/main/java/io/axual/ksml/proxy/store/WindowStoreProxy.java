package io.axual.ksml.proxy.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.HostAccess;

import java.time.Instant;

/**
 * A proxy for accessing Kafka Streams WindowStore in Python code. This proxy mediates between Python and Java data
 * types and delegates all operations to the underlying store.
 */
public class WindowStoreProxy extends AbstractStateStoreProxy<WindowStore<Object, Object>> {

    public WindowStoreProxy(WindowStore<Object, Object> delegate) {
        super(delegate);
    }

    // ==================== ReadOnlyWindowStore methods ====================

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> all() {
        throw new UnsupportedOperationException("all() is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardAll() {
        throw new UnsupportedOperationException("backwardAll() is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public Object fetch(Object key, long time) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(key), time));
    }

    // ==================== WindowStore methods ====================

    @HostAccess.Export
    public WindowStoreIterator<Object> backwardFetch(Object key, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<Object> backwardFetch(Object key, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object keyFrom, Object keyTo, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetchAll(long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("backwardFetchAll(long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetchAll(Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("backwardFetchAll(Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetch(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<Object> fetch(Object key, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetch(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public WindowStoreIterator<Object> fetch(Object key, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetch(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object keyFrom, Object keyTo, Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetch(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetchAll(long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetchAll(long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetchAll(Instant timeFrom, Instant timeTo) {
        throw new UnsupportedOperationException("fetchAll(Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public void put(Object key, Object value, long windowStartTimestamp) {
        delegate.put(NATIVE_MAPPER.fromPython(key), NATIVE_MAPPER.fromPython(value), windowStartTimestamp);
    }
}
