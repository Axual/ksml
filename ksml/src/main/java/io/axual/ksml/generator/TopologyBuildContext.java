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
import io.axual.ksml.definition.*;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.*;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserTimestampExtractor;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.AutoOffsetReset;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

// This is a supporting class during topology building/generation. It contains the main reference to Kafka Streams'
// StreamsBuilder and serves as the lookup point for topology resources. It also contains the Python context in which
// all topology definitions will get loaded. By convention, every KSML definition file is built separately and its
// all its functions are loaded into a private Python context.
public class TopologyBuildContext {
    private final StreamsBuilder builder;
    private final TopologyResources resources;
    private final PythonContext pythonContext;
    @Getter
    private final DataObjectConverter converter = new DataObjectConverter();

    // All wrapped KStreams, KTables and KGlobalTables
    private final Map<String, StreamWrapper> streamWrappersByName = new HashMap<>();
    private final Map<String, StreamWrapper> streamWrappersByTopic = new HashMap<>();

    public TopologyBuildContext(StreamsBuilder builder, TopologyResources resources) {
        this(builder, resources, PythonContextConfig.builder().build());
    }

    public TopologyBuildContext(StreamsBuilder builder,
                                TopologyResources resources,
                                PythonContextConfig pcConfig) {
        this.builder = builder;
        this.resources = resources;
        this.pythonContext = new PythonContext(pcConfig);
    }

    public String namespace() {
        return resources.namespace();
    }

    public <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store).materialized();
    }

    public <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store).materialized();
    }

    public <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store) {
        resources.register(store.name(), store);
        return StoreUtil.<V>materialize(store).materialized();
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

    public StreamWrapper getStreamWrapper(TopologyResource<TopicDefinition> resource) {
        return getStreamWrapper(resource.name(), resource.definition());
    }

    public StreamWrapper getStreamWrapper(final String name, final TopicDefinition topicDefinition) {
        // First do a lookup by name
        if (name != null) {
            if (streamWrappersByName.containsKey(name)) {
                final var expectedResultClass = getStreamWrapperClass(topicDefinition, BaseStreamWrapper.class);
                return validateStreamWrapper(streamWrappersByName.get(name), topicDefinition, expectedResultClass);
            }
            if (topicDefinition == null) {
                throw new TopologyException("Stream '" + name + "' not found");
            }
        }

        // Then do a lookup by topic
        if (topicDefinition != null) {
            if (topicDefinition.topic() == null) {
                throw new TopologyException("Topic definition does not contain a topic name");
            }

            // Get the wrapper from the lookup table, or by creating a new instance
            final Class<? extends BaseStreamWrapper> expectedResultClass;
            final StreamWrapper result;
            if (streamWrappersByTopic.containsKey(topicDefinition.topic())) {
                expectedResultClass = getStreamWrapperClass(topicDefinition, BaseStreamWrapper.class);
                result = streamWrappersByTopic.get(topicDefinition.topic());
            } else {
                expectedResultClass = getStreamWrapperClass(topicDefinition, KStreamWrapper.class);
                if (topicDefinition instanceof StreamDefinition || topicDefinition instanceof TableDefinition || topicDefinition instanceof GlobalTableDefinition) {
                    result = getStreamWrapper(topicDefinition, getStreamWrapperClass(topicDefinition, null));
                } else {
                    result = getStreamWrapper(new StreamDefinition(topicDefinition.topic(), topicDefinition.keyType(), topicDefinition.valueType(), topicDefinition.resetPolicy(), topicDefinition.tsExtractor(), topicDefinition.partitioner()));
                }
            }

            // If the name was not yet registered, do that here
            if (name != null) streamWrappersByName.put(name, result);
            return validateStreamWrapper(result, topicDefinition, expectedResultClass);
        }

        throw new TopologyException("Failed to lookup nameless stream for nameless topic");
    }

    private static Class<? extends BaseStreamWrapper> getStreamWrapperClass(TopicDefinition topicDefinition, Class<? extends BaseStreamWrapper> defaultClass) {
        // Anonymous topics are assumed to be Streams, so treat as if the topic was a stream definition
        Class<? extends BaseStreamWrapper> resultClass = defaultClass;
        // Other types are explicitly assigned
        if (topicDefinition instanceof StreamDefinition) resultClass = KStreamWrapper.class;
        if (topicDefinition instanceof TableDefinition) resultClass = KTableWrapper.class;
        if (topicDefinition instanceof GlobalTableDefinition) resultClass = GlobalKTableWrapper.class;
        return resultClass;
    }

    public KStreamWrapper getStreamWrapper(StreamDefinition stream) {
        return (KStreamWrapper) getStreamWrapper(stream, KStreamWrapper.class);
    }

    public KTableWrapper getStreamWrapper(TableDefinition table) {
        return (KTableWrapper) getStreamWrapper(table, KTableWrapper.class);
    }

    public GlobalKTableWrapper getStreamWrapper(GlobalTableDefinition globalTable) {
        return (GlobalKTableWrapper) getStreamWrapper(globalTable, GlobalKTableWrapper.class);
    }

    public StreamWrapper getStreamWrapper(TopicDefinition definition, Class<? extends BaseStreamWrapper> resultClass) {
        // We do not know the name of the StreamWrapper here, only its definition (which may be inlined in KSML), so we
        // perform a lookup based on the topic name. If we find it, we return that StreamWrapper. If not, we create it,
        // register it and return it here.
        var result = streamWrappersByTopic.get(definition.topic());
        if (result == null) {
            result = buildWrapper(definition.topic(), definition);
            streamWrappersByTopic.put(definition.topic(), result);
        }
        if (!resultClass.isInstance(result)) {
            throw new TopologyException("Stream is of incorrect dataType " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
        }
        return result;
    }

    private <K, V> Consumed<K, V> consumedOf(String name, Serde<K> keySerde, Serde<V> valueSerde, FunctionDefinition tsExtractor, AutoOffsetReset resetPolicy) {
        var result = Consumed.<K, V>as(name);
        if (keySerde != null) result = result.withKeySerde(keySerde);
        if (valueSerde != null) result = result.withValueSerde(valueSerde);
        if (tsExtractor != null) {
            final var tags = defaultMetricTags();
            result = result.withTimestampExtractor(new UserTimestampExtractor(createUserFunction(tsExtractor), tags.append("topic", name)));
        }
        if (resetPolicy != null) result = result.withOffsetResetPolicy(resetPolicy);
        return result;
    }

    private StreamWrapper buildWrapper(String name, TopicDefinition def) {
        if (def instanceof StreamDefinition streamDefinition) {
            final var streamKey = new StreamDataType(streamDefinition.keyType(), true);
            final var streamValue = new StreamDataType(streamDefinition.valueType(), false);
            final var consumed = consumedOf(name, streamKey.serde(), streamValue.serde(), def.tsExtractor(), def.resetPolicy());
            return new KStreamWrapper(builder.stream(streamDefinition.topic(), consumed), streamKey, streamValue);
        }

        if (def instanceof TableDefinition tableDefinition) {
            final var streamKey = new StreamDataType(tableDefinition.keyType(), true);
            final var streamValue = new StreamDataType(tableDefinition.valueType(), false);
            final var store = tableDefinition.store() != null
                    ? tableDefinition.store()
                    // Set up dummy store for tables, mapping to the topic itself, so we don't require an extra state store topic
                    : new KeyValueStateStoreDefinition(tableDefinition.topic(), false, false, false, Duration.ofSeconds(900), Duration.ofSeconds(60), streamKey.userType(), streamValue.userType(), false, false);
            final var mat = StoreUtil.materialize(store);
            final var consumed = consumedOf(name, mat.keySerde(), mat.valueSerde(), def.tsExtractor(), def.resetPolicy());
            return new KTableWrapper(builder.table(tableDefinition.topic(), consumed, mat.materialized()), streamKey, streamValue);
        }

        if (def instanceof GlobalTableDefinition globalTableDefinition) {
            final var streamKey = new StreamDataType(globalTableDefinition.keyType(), true);
            final var streamValue = new StreamDataType(globalTableDefinition.valueType(), false);
            final var store = globalTableDefinition.store() != null
                    ? globalTableDefinition.store()
                    // Set up dummy store for globalTables, mapping to the topic itself, so we don't require an extra state store topic
                    : new KeyValueStateStoreDefinition(globalTableDefinition.topic(), false, false, false, Duration.ofSeconds(900), Duration.ofSeconds(60), streamKey.userType(), streamValue.userType(), false, false);
            final var mat = StoreUtil.materialize(store);
            final var consumed = consumedOf(name, mat.keySerde(), mat.valueSerde(), def.tsExtractor(), def.resetPolicy());
            return new GlobalKTableWrapper(builder.globalTable(globalTableDefinition.topic(), consumed, mat.materialized()), streamKey, streamValue);
        }

        throw new TopologyException("Unknown stream type: " + def.getClass().getSimpleName());
    }

    private StreamWrapper validateStreamWrapper(StreamWrapper wrapper, TopicDefinition definition, Class<? extends BaseStreamWrapper> resultType) {
        if (definition != null) {
            final var topic = definition.topic() != null ? definition.topic() : "unknown topic";
            final var defKeyType = definition.keyType();
            final var wrapperKeyType = wrapper.keyType().userType();
            if (defKeyType != null && !defKeyType.dataType().isAssignableFrom(wrapperKeyType.dataType())) {
                throw new TopologyException("Expected keyType " + defKeyType + " for topic " + definition.topic() + " differs from real keyType " + wrapperKeyType);
            }
            final var defValueType = definition.valueType();
            final var wrapperValueType = wrapper.valueType().userType();
            if (defValueType != null && !defValueType.dataType().isAssignableFrom(wrapperValueType.dataType())) {
                throw new TopologyException("Expected valueType " + defValueType + " for topic '" + topic + "' differs from real valueType " + wrapperValueType);
            }
        }

        if (!resultType.isInstance(wrapper)) {
            throw new TopologyException("Incorrect stream type referenced: expected=" + resultType + ", found=" + wrapper.toString());
        }

        return wrapper;
    }

    // Results of pipelines can be registered in this build context for later use (see AsOperation). This is the entry
    // point for that operation to register pipeline outcomes as independent stream wrappers.
    public void registerStreamWrapper(String name, StreamWrapper wrapper) {
        if (name == null) {
            throw new TopologyException("Can not register " + wrapper.toString() + " without a name");
        }
        if (streamWrappersByName.containsKey(name)) {
            throw new TopologyException("Can not register " + wrapper.toString() + " as " + name + ": name must be unique");
        }
        streamWrappersByName.put(name, wrapper);
    }

    public MetricTags defaultMetricTags() {
        return new MetricTags().append("namespace", namespace());
    }

    // Create a new function in the Python context, using the definition in the parameter
    public UserFunction createUserFunction(FunctionDefinition definition) {
        return PythonFunction.forFunction(pythonContext, resources.namespace(), definition.name(), definition);
    }
}
