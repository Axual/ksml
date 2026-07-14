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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.definition.AggregatorDefinition;
import io.axual.ksml.definition.DefinitionConstants;
import io.axual.ksml.definition.ForEachActionDefinition;
import io.axual.ksml.definition.ForeignKeyExtractorDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.InitializerDefinition;
import io.axual.ksml.definition.KeyTransformerDefinition;
import io.axual.ksml.definition.KeyValueMapperDefinition;
import io.axual.ksml.definition.KeyValuePrinterDefinition;
import io.axual.ksml.definition.KeyValueToKeyValueListTransformerDefinition;
import io.axual.ksml.definition.KeyValueToValueListTransformerDefinition;
import io.axual.ksml.definition.KeyValueTransformerDefinition;
import io.axual.ksml.definition.MergerDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.MetadataTransformerDefinition;
import io.axual.ksml.definition.PredicateDefinition;
import io.axual.ksml.definition.ReducerDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.StreamPartitionerDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.definition.TopicNameExtractorDefinition;
import io.axual.ksml.definition.ValueJoinerDefinition;
import io.axual.ksml.definition.ValueTransformerDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedCogroupedKStreamWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.TimeWindowedCogroupedKStreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared helpers for unit-testing {@link BaseOperation} subclasses without a running Kafka Streams
 * topology or the GraalVM Python runtime.
 *
 * <p>Operations are exercised by calling their {@code apply(...)} methods with mocked Kafka Streams
 * inputs and a mocked {@link TopologyBuildContext}. This builds the topology node (the code under
 * test) but never executes user functions, so the tests run on any JVM.
 */
final class OperationTestSupport {

    static final UserType UNKNOWN_TYPE = UserType.UNKNOWN;

    private OperationTestSupport() {
    }

    /**
     * Registers a mock default notation returning a mock Serde. Operations that build
     * Grouped/Produced/Joined/Repartitioned instances call {@link StreamDataType#serde()}, which
     * resolves a notation from the global (shared) library, so {@link #mockContext()} registers it
     * before every {@code apply}. The entry is snapshotted and restored per test by
     * {@code DefaultNotationIsolationExtension}, so it does not leak into other test classes.
     */
    @SuppressWarnings("unchecked")
    private static void registerMockDefaultNotation() {
        final var notation = mock(Notation.class);
        final Serde<Object> serde = mock(Serde.class);
        when(notation.serde(any(), anyBoolean())).thenReturn(serde);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, notation);
    }

    // --- Types -----------------------------------------------------------------------------------

    static StreamDataType key() {
        return new StreamDataType(UNKNOWN_TYPE, true);
    }

    static StreamDataType value() {
        return new StreamDataType(UNKNOWN_TYPE, false);
    }

    // --- Operation configs -----------------------------------------------------------------------

    static MetricTags tags() {
        return new MetricTags();
    }

    static OperationConfig operationConfig(String name) {
        return new OperationConfig(name, tags());
    }

    static StoreOperationConfig storeConfig(String name) {
        return new StoreOperationConfig(name, tags(), null);
    }

    static StoreOperationConfig storeConfig(String name, io.axual.ksml.definition.StateStoreDefinition store) {
        return new StoreOperationConfig(name, tags(), store);
    }

    static DualStoreOperationConfig dualStoreConfig(String name) {
        return new DualStoreOperationConfig(name, tags(), null, null);
    }

    static DualStoreOperationConfig dualStoreConfig(String name, io.axual.ksml.definition.StateStoreDefinition thisStore, io.axual.ksml.definition.StateStoreDefinition otherStore) {
        return new DualStoreOperationConfig(name, tags(), thisStore, otherStore);
    }

    // --- State store definitions -----------------------------------------------------------------

    static KeyValueStateStoreDefinition keyValueStore(String name) {
        return new KeyValueStateStoreDefinition(name, UNKNOWN_TYPE, UNKNOWN_TYPE);
    }

    static SessionStateStoreDefinition sessionStore(String name) {
        return new SessionStateStoreDefinition(name, false, false, Duration.ofSeconds(60), UNKNOWN_TYPE, UNKNOWN_TYPE, false, false);
    }

    static WindowStateStoreDefinition windowStore(String name) {
        return new WindowStateStoreDefinition(name, false, false, Duration.ofSeconds(60), Duration.ofSeconds(10), false, UNKNOWN_TYPE, UNKNOWN_TYPE, false, false);
    }

    /**
     * Stream-stream joins require a window store that retains duplicates, whose window size is twice
     * the join time difference and whose retention equals {@code 2*timeDifference + grace}. The join
     * tests use a 1s time difference with no grace, hence a 2s window and 2s retention.
     */
    static WindowStateStoreDefinition joinWindowStore(String name) {
        return new WindowStateStoreDefinition(name, false, false, Duration.ofSeconds(2), Duration.ofSeconds(2), true, UNKNOWN_TYPE, UNKNOWN_TYPE, false, false);
    }

    // --- Function definitions --------------------------------------------------------------------

    /** A minimal, fully typed base function definition with the given result type. */
    static FunctionDefinition genericFunction(UserType resultType) {
        return FunctionDefinition.as(
                "generic",
                "fn",
                List.of(),
                (String) null,
                (String) null,
                (String) null,
                resultType,
                List.of());
    }

    static FunctionDefinition genericFunction() {
        return genericFunction(UNKNOWN_TYPE);
    }

    static ReducerDefinition reducer() {
        return new ReducerDefinition(genericFunction());
    }

    static AggregatorDefinition aggregator() {
        return new AggregatorDefinition(genericFunction());
    }

    static InitializerDefinition initializer() {
        return new InitializerDefinition(genericFunction());
    }

    static MergerDefinition merger() {
        return new MergerDefinition(genericFunction());
    }

    static PredicateDefinition predicate() {
        return new PredicateDefinition(genericFunction(new UserType(DataBoolean.DATATYPE)));
    }

    static KeyValueMapperDefinition keyValueMapper() {
        return new KeyValueMapperDefinition(genericFunction());
    }

    /** A key/value selector whose result is a two-element tuple, as required by KTable groupBy. */
    static KeyValueMapperDefinition tupleKeyValueMapper() {
        return new KeyValueMapperDefinition(genericFunction(new UserType(new UserTupleType(UNKNOWN_TYPE, UNKNOWN_TYPE))));
    }

    static ValueJoinerDefinition valueJoiner() {
        return new ValueJoinerDefinition(genericFunction());
    }

    static ForeignKeyExtractorDefinition foreignKeyExtractor() {
        return new ForeignKeyExtractorDefinition(genericFunction());
    }

    static KeyTransformerDefinition keyTransformer() {
        return new KeyTransformerDefinition(genericFunction());
    }

    static ValueTransformerDefinition valueTransformer() {
        return new ValueTransformerDefinition(genericFunction());
    }

    static KeyValueTransformerDefinition keyValueTransformer() {
        return new KeyValueTransformerDefinition(genericFunction(new UserType(new UserTupleType(UNKNOWN_TYPE, UNKNOWN_TYPE))));
    }

    static KeyValueToKeyValueListTransformerDefinition keyValueToKeyValueListTransformer() {
        return new KeyValueToKeyValueListTransformerDefinition(
                genericFunction(new UserType(new ListType(new UserTupleType(UNKNOWN_TYPE, UNKNOWN_TYPE)))));
    }

    static KeyValueToValueListTransformerDefinition keyValueToValueListTransformer() {
        return new KeyValueToValueListTransformerDefinition(genericFunction(new UserType(new ListType(DataType.UNKNOWN))));
    }

    static MetadataTransformerDefinition metadataTransformer() {
        return new MetadataTransformerDefinition(genericFunction(new UserType(DefinitionConstants.METADATA_TYPE)));
    }

    static ForEachActionDefinition forEachAction() {
        // A forEach action must not declare a result type.
        return new ForEachActionDefinition(genericFunction(null));
    }

    static KeyValuePrinterDefinition keyValuePrinter() {
        return new KeyValuePrinterDefinition(genericFunction(new UserType(DataString.DATATYPE)));
    }

    static TopicNameExtractorDefinition topicNameExtractor() {
        return new TopicNameExtractorDefinition(genericFunction(new UserType(DataString.DATATYPE)));
    }

    static StreamPartitionerDefinition streamPartitioner() {
        return new StreamPartitionerDefinition(genericFunction(new UserType(DataInteger.DATATYPE)));
    }

    // --- Topic definitions (join targets) --------------------------------------------------------

    static StreamDefinition streamDefinition() {
        return new StreamDefinition("joinTopic", UNKNOWN_TYPE, UNKNOWN_TYPE, null, null, null);
    }

    static TableDefinition tableDefinition() {
        return new TableDefinition("joinTopic", UNKNOWN_TYPE, UNKNOWN_TYPE, null, null, null, null);
    }

    static GlobalTableDefinition globalTableDefinition() {
        return new GlobalTableDefinition("joinTopic", UNKNOWN_TYPE, UNKNOWN_TYPE, null, null, null, null);
    }

    // --- Mocked build context --------------------------------------------------------------------

    /**
     * A mocked context whose {@link TopologyBuildContext#createUserFunction} returns a real (but
     * never-invoked) {@link UserFunction} mirroring the requested definition. Mirroring the
     * parameters and result type keeps the user-function wrappers (e.g. UserReducer) happy without
     * needing the GraalVM Python runtime. Join lookups return freshly mocked stream wrappers.
     */
    @SuppressWarnings("unchecked")
    static TopologyBuildContext mockContext() {
        registerMockDefaultNotation();
        final var context = mock(TopologyBuildContext.class);
        when(context.createUserFunction(any())).thenAnswer(invocation -> {
            final FunctionDefinition definition = invocation.getArgument(0);
            return new UserFunction("test", definition.name(), definition.parameters(), definition.resultType(), definition.storeNames());
        });
        when(context.getStreamWrapper(any(StreamDefinition.class))).thenReturn(kStream());
        when(context.getStreamWrapper(any(TableDefinition.class))).thenReturn(kTable());
        when(context.getStreamWrapper(any(GlobalTableDefinition.class))).thenReturn(globalKTable());
        when(context.materialize(any(KeyValueStateStoreDefinition.class))).thenReturn(mock(Materialized.class));
        when(context.materialize(any(SessionStateStoreDefinition.class))).thenReturn(mock(Materialized.class));
        when(context.materialize(any(WindowStateStoreDefinition.class))).thenReturn(mock(Materialized.class));
        return context;
    }

    // --- Mocked stream wrappers ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    static KGroupedStreamWrapper groupedStream() {
        return new KGroupedStreamWrapper(mock(KGroupedStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static KGroupedTableWrapper groupedTable() {
        return new KGroupedTableWrapper(mock(KGroupedTable.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static SessionWindowedKStreamWrapper sessionWindowed() {
        return new SessionWindowedKStreamWrapper(mock(SessionWindowedKStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static TimeWindowedKStreamWrapper timeWindowed() {
        return new TimeWindowedKStreamWrapper(mock(TimeWindowedKStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static KStreamWrapper kStream() {
        return new KStreamWrapper(mock(KStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static KTableWrapper kTable() {
        return new KTableWrapper(mock(KTable.class), key(), value());
    }

    /** The windowed key type produced by windowed aggregations, e.g. as consumed by a windowed suppress. */
    static StreamDataType windowedKey() {
        return new StreamDataType(new UserType(new WindowedType(DataType.UNKNOWN)), true);
    }

    @SuppressWarnings("unchecked")
    static GlobalKTableWrapper globalKTable() {
        return new GlobalKTableWrapper(mock(GlobalKTable.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static CogroupedKStreamWrapper cogroupedStream() {
        return new CogroupedKStreamWrapper(mock(CogroupedKStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static SessionWindowedCogroupedKStreamWrapper sessionWindowedCogrouped() {
        return new SessionWindowedCogroupedKStreamWrapper(mock(SessionWindowedCogroupedKStream.class), key(), value());
    }

    @SuppressWarnings("unchecked")
    static TimeWindowedCogroupedKStreamWrapper timeWindowedCogrouped() {
        return new TimeWindowedCogroupedKStreamWrapper(mock(TimeWindowedCogroupedKStream.class), key(), value());
    }
}
