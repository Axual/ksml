package io.axual.ksml.store;

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
 * A proxy wrapper around a Kafka Streams SessionStore that delegates all operations
 * to the underlying store. This proxy can be used to intercept store operations
 * for security, logging, or other cross-cutting concerns.
 *
 * @param <K> the type of keys
 * @param <AGG> the type of aggregated values
 */
public class SessionStoreProxy<K, AGG> extends AbstractStateStoreProxy<SessionStore<K,AGG>> {

    public SessionStoreProxy(SessionStore<K, AGG> delegate) {
        super(delegate);
    }

    // ==================== SessionStore methods ====================

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom, K keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("findSessions(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K key, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, K, long, long) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(K keyFrom, K keyTo, Instant earliestSessionEndTime, Instant latestSessionStartTime) {
        throw new UnsupportedOperationException("backwardFindSessions(K, K, Instant, Instant) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public Object fetchSession(K key, long sessionStartTime, long sessionEndTime) {
        return toPython(delegate.fetchSession(key, sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public Object fetchSession(K key, Instant sessionStartTime, Instant sessionEndTime) {
        return toPython(delegate.fetchSession(key, sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public void put(Windowed<K> sessionKey, AGG aggregate) {
        delegate.put(sessionKey, aggregate);
    }

    @HostAccess.Export
    public void remove(Windowed<K> sessionKey) {
        delegate.remove(sessionKey);
    }

    // ==================== ReadOnlySessionStore methods ====================

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> fetch(K key) {
        throw new UnsupportedOperationException("fetch(K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> fetch(K keyFrom, K keyTo) {
        throw new UnsupportedOperationException("fetch(K, K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFetch(K key) {
        throw new UnsupportedOperationException("backwardFetch(K) is not supported by this proxy (" + getClass() + ")");
    }

    @HostAccess.Export
    public KeyValueIterator<Windowed<K>, AGG> backwardFetch(K keyFrom, K keyTo) {
        throw new UnsupportedOperationException("backwardFetch(K, K) is not supported by this proxy (" + getClass() + ")");
    }

}
