package io.axual.ksml.operation.processor;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.exception.ExecutionException;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeekProcessorTest {

    private static final String[] NO_STORES = new String[0];

    @Mock
    private FixedKeyProcessorContext<Object, Object> context;

    static FixedKeyRecord<Object, Object> fixedKeyRecord(Object key, Object value) {
        return InternalFixedKeyRecordFactory.create(new Record<>(key, value, 0L));
    }

    @Test
    @DisplayName("peek runs the side-effect action and forwards the record unchanged")
    void invokesActionAndForwardsRecordUnchanged() {
        final var invoked = new AtomicBoolean(false);
        final var processor = new PeekProcessor("peek", (stores, rec) -> invoked.set(true), NO_STORES);
        processor.init(context);

        final var rec = fixedKeyRecord("key", "value");
        processor.process(rec);

        assertThat(invoked).isTrue();
        verify(context).forward(rec);
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("init resolves declared state stores and makes them available to the action")
    void initExposesStateStoreToAction() {
        final KeyValueStore<Object, Object> store = mock(KeyValueStore.class);
        when(context.getStateStore("store")).thenReturn(store);

        final var seenStore = new AtomicReference<Object>();
        final var processor = new PeekProcessor("peek", (stores, rec) -> seenStore.set(stores.get("store")), new String[]{"store"});
        processor.init(context);

        processor.process(fixedKeyRecord("key", "value"));

        verify(context).getStateStore("store");
        assertThat(seenStore.get()).isNotNull();
    }

    @Test
    @DisplayName("init throws an execution exception when a declared state store is missing")
    void initFailsWhenStateStoreIsMissing() {
        when(context.getStateStore("missing")).thenReturn(null);
        final var processor = new PeekProcessor("peek", (stores, rec) -> {
        }, new String[]{"missing"});

        assertThatThrownBy(() -> processor.init(context))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("state store");
    }
}
