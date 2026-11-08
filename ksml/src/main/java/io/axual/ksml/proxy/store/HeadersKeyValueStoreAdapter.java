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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.List;
import java.util.function.Supplier;

/**
 * Shows a headers-aware key-value store as a timestamped key-value store, so {@link TimestampedKeyValueStoreProxy}
 * gives Python the same methods and results on both. Writes keep the headers of the record that is being processed.
 */
class HeadersKeyValueStoreAdapter extends AbstractHeadersStoreAdapter<TimestampedKeyValueStoreWithHeaders<Object, Object>> implements TimestampedKeyValueStore<Object, Object> {

    HeadersKeyValueStoreAdapter(TimestampedKeyValueStoreWithHeaders<Object, Object> delegate, Supplier<Headers> recordHeaders) {
        super(delegate, recordHeaders);
    }

    static ValueAndTimestamp<Object> withoutHeaders(ValueTimestampHeaders<Object> value) {
        return value != null ? ValueAndTimestamp.make(value.value(), value.timestamp()) : null;
    }

    private ValueTimestampHeaders<Object> withHeaders(ValueAndTimestamp<Object> value) {
        return value != null ? ValueTimestampHeaders.make(value.value(), value.timestamp(), recordHeaders()) : null;
    }

    private static KeyValueIterator<Object, ValueAndTimestamp<Object>> withoutHeaders(KeyValueIterator<Object, ValueTimestampHeaders<Object>> iterator) {
        return new ValueMappingIterator<>(iterator, HeadersKeyValueStoreAdapter::withoutHeaders);
    }

    @Override
    public void put(Object key, ValueAndTimestamp<Object> value) {
        delegate.put(key, withHeaders(value));
    }

    @Override
    public ValueAndTimestamp<Object> putIfAbsent(Object key, ValueAndTimestamp<Object> value) {
        return withoutHeaders(delegate.putIfAbsent(key, withHeaders(value)));
    }

    @Override
    public void putAll(List<KeyValue<Object, ValueAndTimestamp<Object>>> entries) {
        delegate.putAll(entries.stream().map(entry -> KeyValue.pair(entry.key, withHeaders(entry.value))).toList());
    }

    @Override
    public ValueAndTimestamp<Object> delete(Object key) {
        return withoutHeaders(delegate.delete(key));
    }

    @Override
    public ValueAndTimestamp<Object> get(Object key) {
        return withoutHeaders(delegate.get(key));
    }

    @Override
    public KeyValueIterator<Object, ValueAndTimestamp<Object>> range(Object from, Object to) {
        return withoutHeaders(delegate.range(from, to));
    }

    @Override
    public KeyValueIterator<Object, ValueAndTimestamp<Object>> reverseRange(Object from, Object to) {
        return withoutHeaders(delegate.reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<Object, ValueAndTimestamp<Object>> all() {
        return withoutHeaders(delegate.all());
    }

    @Override
    public KeyValueIterator<Object, ValueAndTimestamp<Object>> reverseAll() {
        return withoutHeaders(delegate.reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Object, ValueAndTimestamp<Object>> prefixScan(P prefix, PS prefixKeySerializer) {
        return withoutHeaders(delegate.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }
}
