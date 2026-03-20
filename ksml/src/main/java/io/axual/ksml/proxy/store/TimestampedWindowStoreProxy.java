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

import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy for accessing Kafka Streams TimestampedWindowStore in Python code. This proxy mediates between Python and
 * Java data types and delegates all operations to the underlying store.
 */
public class TimestampedWindowStoreProxy extends AbstractStateStoreProxy<TimestampedWindowStore<Object, Object>> {

    public TimestampedWindowStoreProxy(TimestampedWindowStore<Object, Object> delegate) {
        super(delegate);
    }

    // ==================== ReadOnlyWindowStore methods ====================

    @HostAccess.Export
    public java.lang.Object fetch(java.lang.Object key, long time) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(key), time));
    }

    // ==================== WindowStore methods ====================

    @HostAccess.Export
    public void put(Object key, ValueAndTimestamp<Object> value, long windowStartTimestamp) {
        delegate.put(key, value, windowStartTimestamp);
    }

    @HostAccess.Export
    public void put(Object key, Object value, long windowStartTimestamp, long timestamp) {
        delegate.put(key, ValueAndTimestamp.make(value, timestamp), windowStartTimestamp);
    }
}
