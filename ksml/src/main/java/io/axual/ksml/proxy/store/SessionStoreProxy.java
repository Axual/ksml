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

    // ==================== SessionStore methods ====================

    @HostAccess.Export
    public Object fetchSession(Object key, long sessionStartTime, long sessionEndTime) {
        return ProxyUtil.toPython(delegate.fetchSession(NATIVE_MAPPER.fromPython(key), sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public Object fetchSession(Object key, Instant sessionStartTime, Instant sessionEndTime) {
        return ProxyUtil.toPython(delegate.fetchSession(NATIVE_MAPPER.fromPython(key), sessionStartTime, sessionEndTime));
    }
}
