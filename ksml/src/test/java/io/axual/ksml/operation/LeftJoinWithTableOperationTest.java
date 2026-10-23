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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.ForeignKeyExtractorDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.type.UserType;
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
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.function.Function;

import static io.axual.ksml.operation.OperationTestSupport.foreignKeyExtractor;
import static io.axual.ksml.operation.OperationTestSupport.genericFunction;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.keyValueStore;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.saveStream;
import static io.axual.ksml.operation.OperationTestSupport.savedTarget;
import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static io.axual.ksml.operation.OperationTestSupport.streamPartitioner;
import static io.axual.ksml.operation.OperationTestSupport.tableTarget;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static io.axual.ksml.operation.OperationTestSupport.valueJoiner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LeftJoinWithTableOperationTest extends OperationTestBase {

    @Test
    @DisplayName("left-joining a stream with a table delegates to KStream.leftJoin and returns a stream")
    @SuppressWarnings("unchecked")
    void applyToStreamReturnsStream() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableTarget(), null, valueJoiner(), null, null, null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KStreamWrapper.class);
        verify(stream).leftJoin(any(KTable.class), any(ValueJoinerWithKey.class), any(Joined.class));
    }

    @Test
    @DisplayName("leftJoin with a foreignKeyExtractor and a stream as input is rejected, because Kafka Streams would ignore the extractor")
    void applyToStreamWithForeignKeyExtractorIsRejected() {
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableTarget(), foreignKeyExtractor(), valueJoiner(), null, null, null);

        assertThatThrownBy(() -> operation.apply(kStream(), mockContext()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("A leftJoin with \"foreignKeyExtractor\" needs a table as input, but found a stream");
    }

    @Test
    @DisplayName("foreign-key left-joining two tables without a store uses the unmaterialized leftJoin overload")
    @SuppressWarnings("unchecked")
    void applyToTableReturnsTable() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableTarget(), foreignKeyExtractor(), valueJoiner(), null, null, null);

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
                storeConfig("leftJoin", keyValueStore("store")), tableTarget(), foreignKeyExtractor(), valueJoiner(),
                null, streamPartitioner(), streamPartitioner());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        // Foreign-key join with a materialized store and both partitioners.
        verify(table).leftJoin(any(KTable.class), any(Function.class), any(ValueJoiner.class), any(TableJoined.class), any(Materialized.class));
    }

    @Test
    @DisplayName("foreign-key left-join wires in the other-table partitioner even when the primary partitioner is absent")
    @SuppressWarnings("unchecked")
    void applyToTableUsesOtherPartitionerWithoutPrimaryPartitioner() throws NoSuchFieldException, IllegalAccessException {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableTarget(), foreignKeyExtractor(), valueJoiner(),
                null, null, streamPartitioner());

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);

        final var tableJoinedCaptor = ArgumentCaptor.forClass(TableJoined.class);
        verify(table).leftJoin(any(KTable.class), any(Function.class), any(ValueJoiner.class), tableJoinedCaptor.capture());

        // Regression check: userOtherPart used to be derived from the wrong null-check (part != null
        // instead of otherPart != null), silently dropping a present otherPartitioner whenever the
        // primary partitioner was absent.
        final Field otherPartitionerField = TableJoined.class.getDeclaredField("otherPartitioner");
        otherPartitionerField.setAccessible(true);
        assertThat(otherPartitionerField.get(tableJoinedCaptor.getValue())).isNotNull();
    }

    @Test
    @DisplayName("foreign-key left-join wires in the primary partitioner even when the other-table partitioner is absent")
    @SuppressWarnings("unchecked")
    void applyToTableUsesPrimaryPartitionerWithoutOtherPartitioner() throws NoSuchFieldException, IllegalAccessException {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), tableTarget(), foreignKeyExtractor(), valueJoiner(),
                null, streamPartitioner(), null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);

        final var tableJoinedCaptor = ArgumentCaptor.forClass(TableJoined.class);
        verify(table).leftJoin(any(KTable.class), any(Function.class), any(ValueJoiner.class), tableJoinedCaptor.capture());

        // Regression check: the old bug derived userOtherPart from `part != null` instead of
        // `otherPart != null`. When only the primary partitioner was set, it wrapped a null
        // delegate in a UserStreamPartitioner, whose constructor (Invoker) throws "Invoker:
        // function can not be null" immediately -- so operation.apply(...) itself used to fail.
        final Field otherPartitionerField = TableJoined.class.getDeclaredField("otherPartitioner");
        otherPartitionerField.setAccessible(true);
        assertThat(otherPartitionerField.get(tableJoinedCaptor.getValue())).isNull();
    }

    @Test
    @DisplayName("left-joining two tables without a foreign key uses the primary-key named-and-materialized leftJoin overload")
    @SuppressWarnings("unchecked")
    void applyToTableWithoutForeignKeyUsesPrimaryKeyJoin() {
        final KTable<Object, Object> table = mock(KTable.class);
        final var input = new KTableWrapper(table, key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin", keyValueStore("store")), tableTarget(), null, valueJoiner(), null, null, null);

        assertThat(operation.apply(input, mockContext())).isInstanceOf(KTableWrapper.class);
        // Primary-key join uses the named + materialized overload.
        verify(table).leftJoin(any(KTable.class), any(ValueJoiner.class), any(Named.class), any(Materialized.class));
    }

    @Test
    @DisplayName("left-joining a stream with a table saved with \"as\" looks the table up by name")
    @SuppressWarnings("unchecked")
    void applyToStreamWithSavedTable() {
        final KStream<Object, Object> stream = mock(KStream.class);
        final var input = new KStreamWrapper(stream, key(), value());
        final var context = mockContext();
        saveStream(context, "saved_table", kTable());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), savedTarget("saved_table"), null, valueJoiner(), null, null, null);

        assertThat(operation.apply(input, context)).isInstanceOf(KStreamWrapper.class);
        verify(stream).leftJoin(any(KTable.class), any(ValueJoinerWithKey.class), any(Joined.class));
    }

    @Test
    @DisplayName("leftJoin with an unknown saved table explains how to define or save it")
    void unknownSavedTableIsNotFound() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), savedTarget("missing"), null, valueJoiner(), null, null, null);

        assertThatThrownBy(() -> operation.apply(input, mockContext()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("LeftJoin table \"missing\" not found")
                .hasMessageContaining("Define it under \"tables\", or save a table with \"as: missing\" in a pipeline defined before this one");
    }

    @Test
    @DisplayName("leftJoin with a saved stream as table is rejected")
    void savedStreamIsNotATable() {
        final var input = new KStreamWrapper(mock(KStream.class), key(), value());
        final var context = mockContext();
        saveStream(context, "saved_stream", kStream());
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), savedTarget("saved_stream"), null, valueJoiner(), null, null, null);

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("LeftJoin table \"saved_stream\" must be a table, but found");
    }

    @Test
    @DisplayName("leftJoin of two tables by primary key with different key types is rejected")
    void applyToTableByPrimaryKeyWithDifferentKeyTypesIsRejected() {
        final var input = new KTableWrapper(mock(KTable.class), typedKey(DataString.DATATYPE), value());
        final var context = mockContext();
        saveStream(context, "other_table", new KTableWrapper(mock(KTable.class), typedKey(DataLong.DATATYPE), value()));
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), savedTarget("other_table"), null, valueJoiner(), null, null, null);

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("LeftJoin table keyType is expected to be of type string, but found long");
    }

    @Test
    @DisplayName("leftJoin of two tables with a foreignKeyExtractor that does not return the other table's key type is rejected")
    void applyToTableWithWrongForeignKeyExtractorResultTypeIsRejected() {
        final var input = new KTableWrapper(mock(KTable.class), typedKey(DataLong.DATATYPE), value());
        final var context = mockContext();
        saveStream(context, "other_table", new KTableWrapper(mock(KTable.class), typedKey(DataString.DATATYPE), value()));
        final var extractor = new ForeignKeyExtractorDefinition(genericFunction(new UserType(DataLong.DATATYPE)));
        final var operation = new LeftJoinWithTableOperation(
                storeConfig("leftJoin"), savedTarget("other_table"), extractor, valueJoiner(), null, null, null);

        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("ForeignKeyExtractor resultType is expected to be assignable to string, but found long");
    }

    private static StreamDataType typedKey(DataType type) {
        return new StreamDataType(new UserType(type), true);
    }
}
