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
import org.apache.kafka.streams.state.SessionStore;
import org.graalvm.polyglot.HostAccess;

import java.time.Instant;

/**
 * A proxy for accessing Kafka Streams SessionStore in Python code. This proxy mediates between Python and Java data
 * types and delegates all operations to the underlying store.
 */
public class SessionStoreProxy extends AbstractStateStoreProxy<SessionStore<Object, Object>> {

    public SessionStoreProxy(SessionStore<Object, Object> delegate) {
        super(delegate);
    }

    // ==================== ReadOnlySessionStore methods ====================

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object key) {
        throw new UnsupportedOperationException("backwardFetch(K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object keyFrom, Object keyTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object key) {
        throw new UnsupportedOperationException("fetch(K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object keyFrom, Object keyTo) {
        throw new UnsupportedOperationException("fetch(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    // ==================== SessionStore methods ====================

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object keyFrom, Object keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object keyFrom, Object keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public Object fetchSession(Object key, long sessionStartTime, long sessionEndTime) {
        return ProxyUtil.toPython(delegate.fetchSession(NATIVE_MAPPER.fromPython(key), sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public Object fetchSession(Object key, Instant sessionStartTime, Instant sessionEndTime) {
        return ProxyUtil.toPython(delegate.fetchSession(NATIVE_MAPPER.fromPython(key), sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object keyFrom, Object keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object keyFrom, Object keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public void put(Windowed<Object> sessionKey, Object aggregate) {
        throw new UnsupportedOperationException("put(Windowed<K>, AGG) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public void remove(Windowed<Object> sessionKey) {
        throw new UnsupportedOperationException("remove(Windowed<K>) is not supported by this proxy (" + getClass() + ")");
    }
}
