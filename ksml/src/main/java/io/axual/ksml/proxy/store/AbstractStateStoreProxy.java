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

import io.axual.ksml.proxy.base.AbstractProxy;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonNativeMapper;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.graalvm.polyglot.HostAccess;

/**
 * Base class for state store proxy implementations.
 * This class exposes the base state store methods for use in KSML Python code. All methods are
 * forwarded to the delegate, except for query and getPosition, which are not supported yet in any proxy.
 */
public abstract class AbstractStateStoreProxy<T extends StateStore> implements StateStore, AbstractProxy {
    protected static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    protected static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    protected final T delegate;

    protected AbstractStateStoreProxy(T delegate) {
        this.delegate = delegate;
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
}
