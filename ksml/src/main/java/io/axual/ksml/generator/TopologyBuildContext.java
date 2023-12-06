package io.axual.ksml.generator;

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

import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TopologyBuildContext {
    private final StreamsBuilder builder;
    private final TopologyResources resources;
    private final PythonContext pythonContext;
    private final String namePrefix;

    // All wrapped KStreams, KTables and KGlobalTables
    private final Map<String, StreamWrapper> streamWrappers = new HashMap<>();

    public TopologyBuildContext(StreamsBuilder builder, TopologyResources resources, String namePrefix) {
        this.builder = builder;
        this.resources = resources;
        this.namePrefix = namePrefix;
        this.pythonContext = new PythonContext();
    }

    public DataObjectConverter getDataObjectConverter() {
        return new DataObjectConverter();
    }

    public <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store, getTopicName(store)).materialized();
    }

    public <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store, getTopicName(store)).materialized();
    }

    public <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store, getTopicName(store)).materialized();
    }

    private static String getTopicName(StateStoreDefinition store) {
        if (store.logging()) return store.name();
        return null;
    }

    public void createUserStateStore(StateStoreDefinition store) {
        if (store instanceof KeyValueStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof SessionStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof WindowStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef);
            builder.addStateStore(storeBuilder);
        }
    }

    public BaseStreamWrapper getStreamWrapper(TopicDefinition topic) {
        if (topic instanceof StreamDefinition) return getStreamWrapper(topic, KStreamWrapper.class);
        if (topic instanceof TableDefinition) return getStreamWrapper(topic, KTableWrapper.class);
        if (topic instanceof GlobalTableDefinition) return getStreamWrapper(topic, GlobalKTableWrapper.class);
        // Anonymous topics are assumed to be Streams, so treat as if the topic was a stream definition
        return getStreamWrapper(new StreamDefinition(topic.topic, topic.keyType, topic.valueType));
    }

    public KStreamWrapper getStreamWrapper(StreamDefinition stream) {
        return getStreamWrapper(stream, KStreamWrapper.class);
    }

    public KTableWrapper getStreamWrapper(TableDefinition table) {
        return getStreamWrapper(table, KTableWrapper.class);
    }

    public GlobalKTableWrapper getStreamWrapper(GlobalTableDefinition globalTable) {
        return getStreamWrapper(globalTable, GlobalKTableWrapper.class);
    }

    public <T extends BaseStreamWrapper> T getStreamWrapper(TopicDefinition definition, Class<T> resultClass) {
        // We do not know the name of the StreamWrapper here, only its definition (which may be inlined in KSML), so we
        // perform a lookup based on the topic name. If we find it, we return that StreamWrapper. If not, we create it,
        // register it and return it here.
        var result = streamWrappers.get(definition.topic());
        if (result == null) {
            result = buildWrapper(definition.topic(), definition);
            streamWrappers.put(definition.topic(), result);
        }
        if (!resultClass.isInstance(result)) {
            throw new KSMLTopologyException("Stream is of incorrect dataType " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
        }
        return (T) result;
    }

    private StreamWrapper buildWrapper(String name, TopicDefinition def) {
        if (def instanceof StreamDefinition streamDefinition) {
            var streamKey = new StreamDataType(streamDefinition.keyType(), true);
            var streamValue = new StreamDataType(streamDefinition.valueType(), false);
            return new KStreamWrapper(
                    builder.stream(streamDefinition.topic(), Consumed.with(streamKey.serde(), streamValue.serde()).withName(name)),
                    streamKey,
                    streamValue);
        }

        if (def instanceof TableDefinition tableDefinition) {
            final var streamKey = new StreamDataType(tableDefinition.keyType(), true);
            final var streamValue = new StreamDataType(tableDefinition.valueType(), false);

            if (tableDefinition.store() != null) {
                final var mat = StoreUtil.materialize(tableDefinition.store(), tableDefinition.topic);
                return new KTableWrapper(builder.table(tableDefinition.topic(), mat.materialized()), streamKey, streamValue);
            }

            // Set up dummy materialization for tables, mapping to the topic itself so we don't require an extra state store topic
            final var store = new KeyValueStateStoreDefinition(tableDefinition.topic, false, false, false, Duration.ofSeconds(900), Duration.ofSeconds(60), streamKey.userType(), streamValue.userType(), false, false);
            final var mat = StoreUtil.materialize(store, tableDefinition.topic);
            final var consumed = Consumed.as(name).withKeySerde(mat.keySerde()).withValueSerde(mat.valueSerde());
            return new KTableWrapper(builder.table(tableDefinition.topic(), consumed, mat.materialized()), streamKey, streamValue);
        }

        if (def instanceof GlobalTableDefinition globalTableDefinition) {
            final var streamKey = new StreamDataType(globalTableDefinition.keyType(), true);
            final var streamValue = new StreamDataType(globalTableDefinition.valueType(), false);
            final var consumed = Consumed.as(name).withKeySerde(streamKey.serde()).withValueSerde(streamValue.serde());
            return new GlobalKTableWrapper(builder.globalTable(globalTableDefinition.topic(), consumed), streamKey, streamValue);
        }

        throw FatalError.topologyError("Unknown stream type: " + def.getClass().getSimpleName());
    }

    public UserFunction createUserFunction(FunctionDefinition definition) {
        return PythonFunction.fromNamed(pythonContext, definition.name(), definition);
    }
}
