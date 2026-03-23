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

import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy for accessing Kafka Streams TimestampedKeyValueStore in Python code. This proxy mediates between Python and
 * Java data types and delegates all operations to the underlying store.
 */
public class TimestampedKeyValueStoreProxy extends AbstractStateStoreProxy<TimestampedKeyValueStore<Object, Object>> {

    @HostAccess.Export
    public TimestampedKeyValueStoreProxy(TimestampedKeyValueStore<Object, Object> delegate) {
        super(delegate);
    }

    @HostAccess.Export
    public Object delete(Object key) {
        return ProxyUtil.toPython(delegate.delete(NATIVE_MAPPER.fromPython(key)));
    }

    @HostAccess.Export
    public Object get(Object key) {
        return ProxyUtil.toPython(delegate.get(NATIVE_MAPPER.fromPython(key)));
    }

    @HostAccess.Export
    public void put(Object key, Object value, long timestamp) {
        delegate.put(DATA_OBJECT_MAPPER.toDataObject(key), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(value), timestamp));
    }

    @HostAccess.Export
    public Object putIfAbsent(Object key, Object value, long timestamp) {
        return ProxyUtil.toPython(delegate.putIfAbsent(DATA_OBJECT_MAPPER.toDataObject(key), ValueAndTimestamp.make(DATA_OBJECT_MAPPER.toDataObject(value), timestamp)));
    }

    @HostAccess.Export
    public Object approximateNumEntries() {
        return ProxyUtil.toPython(delegate.approximateNumEntries());
    }
}
