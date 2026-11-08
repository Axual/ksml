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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import java.util.function.Supplier;

/**
 * Shows a headers-aware session store as a session store, so {@link SessionStoreProxy} gives Python the same methods
 * and results on both. Writes keep the headers of the record that is being processed.
 */
class HeadersSessionStoreAdapter extends AbstractHeadersStoreAdapter<SessionStoreWithHeaders<Object, Object>> implements SessionStore<Object, Object> {

    HeadersSessionStoreAdapter(SessionStoreWithHeaders<Object, Object> delegate, Supplier<Headers> recordHeaders) {
        super(delegate, recordHeaders);
    }

    private static KeyValueIterator<Windowed<Object>, Object> withoutHeaders(KeyValueIterator<Windowed<Object>, AggregationWithHeaders<Object>> iterator) {
        return new ValueMappingIterator<>(iterator, AggregationWithHeaders::getAggregationOrNull);
    }

    @Override
    public void put(Windowed<Object> sessionKey, Object aggregate) {
        delegate.put(sessionKey, aggregate != null ? AggregationWithHeaders.make(aggregate, recordHeaders()) : null);
    }

    @Override
    public void remove(Windowed<Object> sessionKey) {
        delegate.remove(sessionKey);
    }

    @Override
    public Object fetchSession(Object key, long sessionStartTime, long sessionEndTime) {
        return AggregationWithHeaders.getAggregationOrNull(delegate.fetchSession(key, sessionStartTime, sessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object key) {
        return withoutHeaders(delegate.fetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> fetch(Object keyFrom, Object keyTo) {
        return withoutHeaders(delegate.fetch(keyFrom, keyTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object key) {
        return withoutHeaders(delegate.backwardFetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> backwardFetch(Object keyFrom, Object keyTo) {
        return withoutHeaders(delegate.backwardFetch(keyFrom, keyTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object key, long earliestSessionEndTime, long latestSessionStartTime) {
        return withoutHeaders(delegate.findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> findSessions(Object keyFrom, Object keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        return withoutHeaders(delegate.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object key, long earliestSessionEndTime, long latestSessionStartTime) {
        return withoutHeaders(delegate.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, Object> backwardFindSessions(Object keyFrom, Object keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        return withoutHeaders(delegate.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }
}
