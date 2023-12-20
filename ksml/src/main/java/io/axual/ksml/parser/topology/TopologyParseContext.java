package io.axual.ksml.parser.topology;

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
import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
public class TopologyParseContext implements ParseContext {
    private final PythonContext pythonContext;
    private final StreamsBuilder builder;
    private final NotationLibrary notationLibrary;
    private final String namePrefix;

    // All registered KStreams, KTables and KGlobalTables
    private final Map<String, BaseStreamDefinition> streamDefinitions = new HashMap<>();

    // All registered user functions
    private final Map<String, FunctionDefinition> functionDefinitions = new HashMap<>();

    // All registered state stores
    private final Map<String, StateStoreDefinition> stateStoreDefinitions = new HashMap<>();

    // The names of all state stores that were created, either through explicit creation or through the use of a
    // Materialized parameter in any of the Kafka Streams operations.
    private final Set<String> createdStateStores = new HashSet<>();

    // All wrapped KStreams, KTables and KGlobalTables
    private final Map<String, StreamWrapper> streamWrappers = new HashMap<>();
    private final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();
    private final Set<String> knownTopics = new HashSet<>();

    public TopologyParseContext(StreamsBuilder builder, NotationLibrary notationLibrary, String namePrefix) {
        this.builder = builder;
        this.notationLibrary = notationLibrary;
        this.namePrefix = namePrefix;
        this.pythonContext = new PythonContext(new DataObjectConverter(notationLibrary));
    }

    public Set<String> getRegisteredTopics() {
        return knownTopics;
    }

    public void registerTopic(String topic) {
        knownTopics.add(topic);
    }

    public void registerStreamDefinition(String name, BaseStreamDefinition def) {
        if (streamDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Stream definition must be unique: " + name);
        }
        streamDefinitions.put(name, def);
        registerTopic(def.topic);
        if (def instanceof TableDefinition tableDef) {
            // Ensure the local state store is registered with the StreamBuilder
            getStreamWrapper(tableDef);
        }
    }

    public void registerFunction(String name, FunctionDefinition functionDefinition) {
        if (functionDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Function definition must be unique: " + name);
        }
        functionDefinitions.put(name, functionDefinition);

        // If there are any uncreated state stores needed, create them first
        for (final var storeName : functionDefinition.storeNames) {
            final var store = stateStoreDefinitions.get(storeName);
            if (store == null)
                throw new KSMLTopologyException("Function " + name + " uses undefined state store: " + storeName);
            if (!createdStateStores.contains(storeName)) {
                createUserStateStore(store);
            }
        }

        // Preload the function into the Python context
        PythonFunction.fromNamed(pythonContext, name, functionDefinition);
    }

    public void registerStateStore(String name, StateStoreDefinition store) {
        registerStateStore(name, store, false);
    }

    public void registerStateStore(String name, StateStoreDefinition store, boolean canBeDuplicate) {
        // Check if the store is properly named
        if (store.name() == null || store.name().isEmpty()) {
            throw new KSMLTopologyException("StateStore does not have a name: " + store);
        }
        // Check if the name is equal to the store name (otherwise a parsing error occurred)
        if (!store.name().equals(name)) {
            throw new KSMLTopologyException("StateStore name mistmatch: this is a parsing error: " + store);
        }
        // State stores can only be registered once. Duplicate names are a sign of faulty KSML definitions
        if (stateStoreDefinitions.containsKey(store.name())) {
            if (!canBeDuplicate) {
                throw new KSMLTopologyException("StateStore is not unique: " + store.name());
            }
            if (!stateStoreDefinitions.get(store.name()).equals(store)) {
                throw new KSMLTopologyException("StateStore definition conflicts earlier registration: " + store);
            }
        } else {
            stateStoreDefinitions.put(store.name(), store);
        }
    }

    public void registerStateStoreAsCreated(StateStoreDefinition store) {
        registerStateStore(store.name(), store, true);
        markStateStoreCreated(store.name());
    }

    public void markStateStoreCreated(String name) {
        createdStateStores.add(name);
    }

    private StreamWrapper buildWrapper(String name, BaseStreamDefinition def) {
        var wrapper = def.addToBuilder(builder, name, notationLibrary, this::registerStateStoreAsCreated);
        streamWrappers.put(name, wrapper);
        if (!name.equals(def.topic)) {
            streamWrappers.put(def.topic, wrapper);
        }
        return wrapper;
    }

    @Override
    public String getNamePrefix() {
        return namePrefix;
    }

    @Override
    public Map<String, BaseStreamDefinition> getStreamDefinitions() {
        return streamDefinitions;
    }

    @Override
    public <T extends BaseStreamWrapper> T getStreamWrapper(BaseStreamDefinition definition, Class<T> resultClass) {
        // We do not know the name of the StreamWrapper here, only its definition (which may be inlined in KSML), so we
        // perform a lookup based on the topic name. If we find it, we return that StreamWrapper. If not, we create it,
        // register it and return it here.
        var result = streamWrappers.get(definition.topic);
        if (result == null) {
            result = buildWrapper(definition.topic, definition);
        }
        if (!resultClass.isInstance(result)) {
            throw new KSMLTopologyException("Stream is of incorrect dataType " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
        }
        return (T) result;
    }

    @Override
    public BaseStreamWrapper getStreamWrapper(BaseStreamDefinition definition) {
        return getStreamWrapper(definition, BaseStreamWrapper.class);
    }

    @Override
    public Map<String, FunctionDefinition> getFunctionDefinitions() {
        return functionDefinitions;
    }

    @Override
    public UserFunction createAnonUserFunction(String name, FunctionDefinition definition, YamlNode node) {
        return PythonFunction.fromAnon(pythonContext, name, definition, node.getDottedName());
    }
    @Override
    public UserFunction createNamedUserFunction(String name, FunctionDefinition definition) {
        return PythonFunction.fromNamed(pythonContext, name, definition);
    }

    @Override
    public Map<String, StateStoreDefinition> getStoreDefinitions() {
        return stateStoreDefinitions;
    }

    @Override
    public Map<String, AtomicInteger> getTypeInstanceCounters() {
        return typeInstanceCounters;
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }

    private void createUserStateStore(StateStoreDefinition store) {
        final var keyType = new StreamDataType(notationLibrary, store.keyType(), true);
        final var valueType = new StreamDataType(notationLibrary, store.valueType(), false);

        if (store instanceof KeyValueStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof SessionStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof WindowStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        markStateStoreCreated(store.name());
    }

    public Topology build() {
        // Create all state stores that were defined, but not yet implicitly created (eg. through using Materialized)
        for (Map.Entry<String, StateStoreDefinition> entry : stateStoreDefinitions.entrySet()) {
            if (!createdStateStores.contains(entry.getKey())) {
                createUserStateStore(entry.getValue());
                createdStateStores.add(entry.getKey());
            }
        }
        return builder.build();
    }
}