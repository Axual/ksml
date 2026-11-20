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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

/**
 * Base for the adapters that show a headers-aware store as the store type without headers that Python already knows,
 * so the same store proxy and the same Python code work on both. Reads drop the headers; writes store empty headers,
 * as the aggregations and tables of Kafka Streams do.
 *
 * @param <S> the headers-aware store type
 */
abstract class AbstractHeadersStoreAdapter<S extends StateStore> implements StateStore {
    protected final S delegate;

    protected AbstractHeadersStoreAdapter(S delegate) {
        this.delegate = delegate;
    }

    /** The headers that a write stores: none, so the record headers don't go to the store and its changelog topic. */
    protected static Headers storedHeaders() {
        return new RecordHeaders();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(StateStoreContext stateStoreContext, StateStore root) {
        delegate.init(stateStoreContext, root);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }
}
