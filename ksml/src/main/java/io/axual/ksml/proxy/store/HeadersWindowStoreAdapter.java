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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Shows a headers-aware window store as a timestamped window store, so {@link TimestampedWindowStoreProxy} gives
 * Python the same methods and results on both. Writes keep the headers of the record that is being processed.
 */
class HeadersWindowStoreAdapter extends AbstractHeadersStoreAdapter<TimestampedWindowStoreWithHeaders<Object, Object>> implements TimestampedWindowStore<Object, Object> {

    HeadersWindowStoreAdapter(TimestampedWindowStoreWithHeaders<Object, Object> delegate, Supplier<Headers> recordHeaders) {
        super(delegate, recordHeaders);
    }

    private static WindowStoreIterator<ValueAndTimestamp<Object>> withoutHeaders(WindowStoreIterator<ValueTimestampHeaders<Object>> iterator) {
        return new ValueMappingIterator.Window<>(iterator, HeadersKeyValueStoreAdapter::withoutHeaders);
    }

    private static KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> withoutHeaders(KeyValueIterator<Windowed<Object>, ValueTimestampHeaders<Object>> iterator) {
        return new ValueMappingIterator<>(iterator, HeadersKeyValueStoreAdapter::withoutHeaders);
    }

    @Override
    public void put(Object key, ValueAndTimestamp<Object> value, long windowStartTimestamp) {
        final var stored = value != null ? ValueTimestampHeaders.make(value.value(), value.timestamp(), recordHeaders()) : null;
        delegate.put(key, stored, windowStartTimestamp);
    }

    @Override
    public ValueAndTimestamp<Object> fetch(Object key, long time) {
        return HeadersKeyValueStoreAdapter.withoutHeaders(delegate.fetch(key, time));
    }

    @Override
    public WindowStoreIterator<ValueAndTimestamp<Object>> fetch(Object key, long timeFrom, long timeTo) {
        return withoutHeaders(delegate.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<ValueAndTimestamp<Object>> fetch(Object key, Instant timeFrom, Instant timeTo) {
        return withoutHeaders(delegate.fetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> fetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return withoutHeaders(delegate.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> fetch(Object keyFrom, Object keyTo, Instant timeFrom, Instant timeTo) {
        return withoutHeaders(delegate.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<ValueAndTimestamp<Object>> backwardFetch(Object key, long timeFrom, long timeTo) {
        return withoutHeaders(delegate.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> backwardFetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return withoutHeaders(delegate.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> fetchAll(long timeFrom, long timeTo) {
        return withoutHeaders(delegate.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> fetchAll(Instant timeFrom, Instant timeTo) {
        return withoutHeaders(delegate.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> backwardFetchAll(long timeFrom, long timeTo) {
        return withoutHeaders(delegate.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> all() {
        return withoutHeaders(delegate.all());
    }

    @Override
    public KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> backwardAll() {
        return withoutHeaders(delegate.backwardAll());
    }
}
