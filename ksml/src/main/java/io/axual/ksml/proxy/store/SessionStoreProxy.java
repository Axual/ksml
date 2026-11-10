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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.python.PythonErrors;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.SessionStore;
import org.graalvm.polyglot.HostAccess;

import java.math.BigInteger;
import java.util.Map;

import static io.axual.ksml.dsl.WindowedSchema.WINDOWED_SCHEMA_END_FIELD;
import static io.axual.ksml.dsl.WindowedSchema.WINDOWED_SCHEMA_KEY_FIELD;
import static io.axual.ksml.dsl.WindowedSchema.WINDOWED_SCHEMA_START_FIELD;

/**
 * A proxy for accessing Kafka Streams SessionStore in Python code. This proxy mediates between Python and Java data
 * types and delegates all operations to the underlying store.
 */
public class SessionStoreProxy extends AbstractStateStoreProxy<SessionStore<Object, Object>> {
    private static final String SESSION_KEY_FIELDS = "\"key\", \"start\" and \"end\"";

    public SessionStoreProxy(SessionStore<Object, Object> delegate) {
        this(delegate, null, null);
    }

    /** Creates a proxy for a store with these declared key and value types; {@code null} means unknown. */
    public SessionStoreProxy(SessionStore<Object, Object> delegate, DataType keyType, DataType valueType) {
        super(delegate, keyType, valueType);
    }

    // ==================== ReadOnlySessionStore methods ====================

    @HostAccess.Export
    public Object backwardFetch(final Object key) {
        return ProxyUtil.toPython(delegate.backwardFetch(toRequiredKey(key)));
    }

    @HostAccess.Export
    public Object backwardFetch(final Object keyFrom, final Object keyTo) {
        return ProxyUtil.toPython(delegate.backwardFetch(toKey(keyFrom), toKey(keyTo)));
    }

    @HostAccess.Export
    public Object backwardFindSessions(final Object key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return ProxyUtil.toPython(delegate.backwardFindSessions(toRequiredKey(key), earliestSessionEndTime, latestSessionStartTime));
    }

    @HostAccess.Export
    public Object backwardFindSessions(final Object keyFrom, final Object keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return ProxyUtil.toPython(delegate.backwardFindSessions(toKey(keyFrom), toKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
    }

    @HostAccess.Export
    public Object fetch(final Object key) {
        return ProxyUtil.toPython(delegate.fetch(toRequiredKey(key)));
    }

    @HostAccess.Export
    public Object fetch(final Object keyFrom, final Object keyTo) {
        return ProxyUtil.toPython(delegate.fetch(toKey(keyFrom), toKey(keyTo)));
    }

    @HostAccess.Export
    public Object fetchSession(final Object key, final long sessionStartTime, final long sessionEndTime) {
        return ProxyUtil.toPython(delegate.fetchSession(toRequiredKey(key), sessionStartTime, sessionEndTime));
    }

    @HostAccess.Export
    public Object findSessions(Object key, long earliestSessionEndTime, long latestSessionStartTime) {
        return ProxyUtil.toPython(delegate.findSessions(toRequiredKey(key), earliestSessionEndTime, latestSessionStartTime));
    }

    @HostAccess.Export
    public Object findSessions(Object keyFrom, Object keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        return ProxyUtil.toPython(delegate.findSessions(toKey(keyFrom), toKey(keyTo), earliestSessionEndTime, latestSessionStartTime));
    }

    // ==================== SessionStore methods ====================

    @HostAccess.Export
    public void put(final Object sessionKey, final Object aggregate) {
        delegate.put(toSessionKey(sessionKey), toValue(aggregate));
    }

    @HostAccess.Export
    public void remove(final Object sessionKey) {
        delegate.remove(toSessionKey(sessionKey));
    }

    /**
     * Reads a session key: a dict with {@code key}, {@code start} and {@code end}, as reads return it (other fields,
     * such as {@code startTime}, are ignored). The key gets the store's key type, start and end must be ints, and a
     * single-record session has start equal to end. A malformed session key raises TypeError or ValueError.
     */
    private Windowed<Object> toSessionKey(Object sessionKey) {
        final var value = sessionKey instanceof DataObject dataObject
                ? DATA_OBJECT_TO_JAVA.fromDataObject(dataObject)
                : NATIVE_MAPPER.fromPython(sessionKey);
        if (!(value instanceof Map<?, ?> fields))
            throw PythonErrors.typeError(storeError("session key must be a dict with " + SESSION_KEY_FIELDS + ", got " + typeName(value)));
        if (!fields.containsKey(WINDOWED_SCHEMA_KEY_FIELD) || !fields.containsKey(WINDOWED_SCHEMA_START_FIELD) || !fields.containsKey(WINDOWED_SCHEMA_END_FIELD))
            throw PythonErrors.valueError(storeError("session key must have " + SESSION_KEY_FIELDS));
        final var start = toSessionTime(fields.get(WINDOWED_SCHEMA_START_FIELD), WINDOWED_SCHEMA_START_FIELD);
        final var end = toSessionTime(fields.get(WINDOWED_SCHEMA_END_FIELD), WINDOWED_SCHEMA_END_FIELD);
        // Kafka rejects a negative start with an IllegalArgumentException, which Python cannot catch
        if (start < 0) throw PythonErrors.valueError(storeError("session key start must not be negative"));
        if (start > end) throw PythonErrors.valueError(storeError("session key start must not be after end"));
        // Kafka rejects a null key with a NullPointerException, which Python cannot catch
        final var key = toKey(fields.get(WINDOWED_SCHEMA_KEY_FIELD));
        if (key == null) throw PythonErrors.valueError(storeError("session key \"key\" must not be None"));
        return new Windowed<>(key, new SessionWindow(start, end));
    }

    private long toSessionTime(Object time, String field) {
        final var name = "session key " + field;
        if (time instanceof BigInteger)
            throw PythonErrors.valueError(storeError(name + " int is out of range for long"));
        if (!isInteger(time)) throw PythonErrors.typeError(storeError(name + " must be int, got " + typeName(time)));
        return ((Number) time).longValue();
    }
}
