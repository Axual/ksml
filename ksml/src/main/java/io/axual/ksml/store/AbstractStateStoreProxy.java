package io.axual.ksml.store;

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

import io.axual.ksml.python.PythonTypeConverter;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.graalvm.polyglot.HostAccess;

/**
 * Base class for state store proxy implementations.
 * This class exposes the base state store methods for use in KSML Python code. All methods are
 * forwarded to the delegate, except for query and getPosition, which are not supported yet in any proxy.
 */
public abstract class AbstractStateStoreProxy<T extends StateStore> implements StateStore {

    final T delegate;

    public AbstractStateStoreProxy(T delegate1) {
        this.delegate = delegate1;
    }

    /**
     * Convert a value to a Python-compatible type.
     * Maps and Lists are converted to ProxyHashMap/ProxyArray for Python interop.
     *
     * @param value the value to convert
     * @return the Python-compatible value
     */
    protected Object toPython(Object value) {
        return PythonTypeConverter.toPython(value);
    }

    @HostAccess.Export
    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(StateStoreContext stateStoreContext, StateStore root) {
        delegate.init(stateStoreContext, root);
    }

    @HostAccess.Export
    @Override
    public void flush() {
        delegate.flush();
    }

    @HostAccess.Export
    @Override
    public void close() {
        delegate.close();
    }

    @HostAccess.Export
    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    @HostAccess.Export
    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    @HostAccess.Export
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        throw new UnsupportedOperationException(
            "query is not implemented by this StateStore proxy (" + getClass() + ")"
        );
    }

    @Override
    @HostAccess.Export
    public Position getPosition() {
        throw new UnsupportedOperationException(
            "getPosition is not implemented by this StateStore proxy (" + getClass() + ")"
        );
    }

}
