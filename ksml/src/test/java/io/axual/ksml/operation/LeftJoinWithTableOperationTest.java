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

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.axual.ksml.operation.OperationTestSupport.foreignKeyExtractor;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.streamPartitioner;
import static io.axual.ksml.operation.OperationTestSupport.tableDefinition;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.valueJoiner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LeftJoinWithTableOperationTest {

    @Test
    @DisplayName("left-joining a stream with a table delegates to KStream.leftJoin and returns a stream")
    @SuppressWarnings("unchecked")
    void applyToStreamReturnsStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableDefinition(), foreignKeyExtractor(), valueJoiner(), null, null, null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).leftJoin(any(KTable.class), any(ValueJoinerWithKey.class), any(Joined.class));
    }

    @Test
    @DisplayName("foreign-key left-joining two tables without a store uses the unmaterialized leftJoin overload")
    @SuppressWarnings("unchecked")
    void applyToTableReturnsTable() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableDefinition(), foreignKeyExtractor(), valueJoiner(), null, null, null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        // Foreign-key join without a store, so no Materialized, no partitioners.
        verify(table).leftJoin(any(KTable.class), any(Function.class), any(ValueJoiner.class), any(TableJoined.class));
    }

    @Test
    @DisplayName("foreign-key left-joining two tables with a store and partitioners uses the materialized leftJoin overload")
    @SuppressWarnings("unchecked")
    void applyToTableWithStoreAndPartitioners() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin", keyValueStore("store")), tableDefinition(), foreignKeyExtractor(), valueJoiner(),
                null, streamPartitioner(), streamPartitioner());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        // Foreign-key join with a materialized store and both partitioners.
        verify(table).leftJoin(any(KTable.class), any(Function.class), any(ValueJoiner.class), any(TableJoined.class), any(Materialized.class));
    }

    @Test
    @DisplayName("left-joining two tables without a foreign key uses the primary-key named-and-materialized leftJoin overload")
    @SuppressWarnings("unchecked")
    void applyToTableWithoutForeignKeyUsesPrimaryKeyJoin() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin", keyValueStore("store")), tableDefinition(), null, valueJoiner(), null, null, null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        // Primary-key join uses the named + materialized overload.
        verify(table).leftJoin(any(KTable.class), any(ValueJoiner.class), any(Named.class), any(Materialized.class));
    }
}
