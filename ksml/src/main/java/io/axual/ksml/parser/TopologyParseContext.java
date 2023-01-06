package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.internals.GroupedInternal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.MessageProducerDefinition;
import io.axual.ksml.definition.StoreDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.store.StoreType;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

/**
 * Parse context which calls {@link StreamsBuilder} to build up the streams topology and keeps track of the wrapped streams.
 */
public class TopologyParseContext implements ParseContext {
    private final PythonContext pythonContext = new PythonContext();
    private final StreamsBuilder builder;
    private final NotationLibrary notationLibrary;
    private final String namePrefix;
    private final Map<String, BaseStreamDefinition> streamDefinitions = new HashMap<>();
    private final Map<String, FunctionDefinition> functionDefinitions = new HashMap<>();
    private final Map<String, MessageProducerDefinition> producerDefinitions = new HashMap<>();
    private final Map<String, StoreDefinition> storeDefinitions = new HashMap<>();
    private final Map<String, StreamWrapper> streamWrappers = new HashMap<>();
    private final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();
    private final Map<String, GroupedInternal<?, ?>> groupedStores = new HashMap<>();
    private final Map<String, StoreDescriptor> stateStores = new HashMap<>();

    public record StoreDescriptor(StoreType type, StoreDefinition store, StreamDataType keyType,
                                  StreamDataType valueType) {
    }

    public TopologyParseContext(StreamsBuilder builder, NotationLibrary notationLibrary, String namePrefix) {
        this.builder = builder;
        this.notationLibrary = notationLibrary;
        this.namePrefix = namePrefix;
    }

    public void registerStreamDefinition(String name, BaseStreamDefinition def) {
        if (streamDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Stream definition must be unique: " + name);
        }
        streamDefinitions.put(name, def);
        buildWrapper(name, def);
    }

    public void registerFunction(String name, FunctionDefinition functionDefinition) {
        if (functionDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Function definition must be unique: " + name);
        }
        functionDefinitions.put(name, functionDefinition);
    }

    public void registerMessageProducer(String name, MessageProducerDefinition messageProducerDefinition) {
        if (producerDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Producer definition must be unique: " + name);
        }
        producerDefinitions.put(name, messageProducerDefinition);
    }

    public void registerStore(String name, StoreDefinition storeDefinition) {
        if (storeDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Store definition must be unique: " + name);
        }
        storeDefinitions.put(name, storeDefinition);
    }

    private StreamWrapper buildWrapper(String name, BaseStreamDefinition def) {
        var wrapper = def.addToBuilder(builder, name, notationLibrary);
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
    public UserFunction getUserFunction(FunctionDefinition definition, String name) {
        return new PythonFunction(pythonContext, name, definition);
    }

    public Map<String, MessageProducerDefinition> producers() {
        return producerDefinitions;
    }

    @Override
    public Map<String, StoreDefinition> getStoreDefinitions() {
        return storeDefinitions;
    }

    @Override
    public Map<String, AtomicInteger> getTypeInstanceCounters() {
        return typeInstanceCounters;
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }

    @Override
    public <K, V> void registerGrouped(Grouped<K, V> grouped) {
        var copy = new GroupedInternal<>(grouped);
        groupedStores.put(copy.name(), copy);
    }

    @Override
    public void registerStore(StoreType type, StoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        stateStores.put(store.name(), new StoreDescriptor(type, store, keyType, valueType));
    }

    public Map<String, StoreDescriptor> stores() {
        return stateStores;
    }

    public Topology build() {
        return builder.build();
    }
}
