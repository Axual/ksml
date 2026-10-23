package io.axual.ksml.operation;

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

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ToTableOperationTest extends OperationTestBase {

    @Test
    @DisplayName("toTable on a stream produces a KTable result")
    void applyToStreamReturnsTable() {
        final var operation = new ToTableOperation(storeConfig("toTable"));
        assertThat(operation.apply(kStream(), mockContext())).isInstanceOf(KTableWrapper.class);
    }

    @Test
    @DisplayName("toTable on a stream with a configured store materializes that store")
    void applyToStreamWithStoreMaterializes() {
        final var store = keyValueStore("store");
        final var operation = new ToTableOperation(storeConfig("toTable", store));
        final var context = mockContext();

        assertThat(operation.apply(kStream(), context)).isInstanceOf(KTableWrapper.class);
        verify(context).materialize(store);
    }

    @Test
    @DisplayName("toTable on a stream without a store still passes the key and value serdes")
    @SuppressWarnings("unchecked")
    void applyToStreamWithoutStorePassesSerdes() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var operation = new ToTableOperation(storeConfig("toTable"));
        final var context = mockContext();

        assertThat(operation.apply(new KStreamWrapper(stream, key(), value()), context)).isInstanceOf(KTableWrapper.class);

        final var captor = ArgumentCaptor.forClass(Materialized.class);
        verify(stream).toTable(any(), captor.capture());
        verify(context, never()).materialize(any(KeyValueStateStoreDefinition.class));
        // Without serdes, the table's state store fails at startup, because KSML sets no default serdes
        final var materialized = new MaterializedInternal<Object, Object, StateStore>(captor.getValue());
        assertThat(materialized.keySerde()).isNotNull();
        assertThat(materialized.valueSerde()).isNotNull();
        assertThat(materialized.storeName()).isNull();
    }
}
