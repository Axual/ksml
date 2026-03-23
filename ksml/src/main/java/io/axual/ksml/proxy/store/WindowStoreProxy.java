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

import org.apache.kafka.streams.state.WindowStore;
import org.graalvm.polyglot.HostAccess;

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
    public Object all() {
        return ProxyUtil.toPython(delegate.all());
    }

    @HostAccess.Export
    public Object backwardAll() {
        return ProxyUtil.toPython(delegate.backwardAll());
    }

    @HostAccess.Export
    public Object backwardFetch(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.backwardFetch(NATIVE_MAPPER.fromPython(key), timeFrom, timeTo));
    }

    @HostAccess.Export
    public Object backwardFetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.backwardFetch(NATIVE_MAPPER.fromPython(keyFrom), NATIVE_MAPPER.fromPython(keyTo), timeFrom, timeTo));
    }

    @HostAccess.Export
    public Object backwardFetchAll(long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.backwardFetchAll(timeFrom, timeTo));
    }

    @HostAccess.Export
    public Object fetch(Object key, long time) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(key), time));
    }

    @HostAccess.Export
    public Object fetch(Object key, long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(key), timeFrom, timeTo));
    }

    @HostAccess.Export
    public Object fetch(Object keyFrom, Object keyTo, long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.fetch(NATIVE_MAPPER.fromPython(keyFrom), NATIVE_MAPPER.fromPython(keyTo), timeFrom, timeTo));
    }

    @HostAccess.Export
    public Object fetchAll(long timeFrom, long timeTo) {
        return ProxyUtil.toPython(delegate.fetchAll(timeFrom, timeTo));
    }

    // ==================== WindowStore methods ====================

    @HostAccess.Export
    public void put(Object key, Object value, long windowStartTimestamp) {
        delegate.put(NATIVE_MAPPER.fromPython(key), NATIVE_MAPPER.fromPython(value), windowStartTimestamp);
    }
}
