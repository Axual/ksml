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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.GroupedInternal;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

/**
 * Parse context which calls {@link StreamsBuilder} to build up the streams topology and keeps track of the wrapped streams.
 */
public class TopologyParseContext implements ParseContext {
    private final StreamsBuilder builder;
    private final NotationLibrary notationLibrary;
    private final Map<String, BaseStreamDefinition> streamDefinitions;
    private final Map<String, FunctionDefinition> functionDefinitions;
    private final Map<String, StreamWrapper> streamWrappers = new HashMap<>();
    private final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();
    private final Map<String, GroupedInternal<?, ?>> groupedStores = new HashMap<>();
    private final Map<String, MaterializedInternal<?, ?, ?>> stateStores = new HashMap<>();

    public TopologyParseContext(StreamsBuilder builder, NotationLibrary notationLibrary, Map<String, BaseStreamDefinition> streamDefinitions, Map<String, FunctionDefinition> functionDefinitions) {
        this.builder = builder;
        this.notationLibrary = notationLibrary;
        this.streamDefinitions = streamDefinitions;
        this.functionDefinitions = functionDefinitions;

        // Generate StreamWrappers for every defined stream
        streamDefinitions.forEach(this::buildWrapper);
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
            throw new KSMLTopologyException("Stream is of incorrect type " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
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
        return new PythonFunction(name, definition);
    }

    @Override
    public Map<String, AtomicInteger> getTypeInstanceCounters() {
        return typeInstanceCounters;
    }

    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }

    public <K, V> void registerGrouped(Grouped<K, V> grouped) {
        var copy = new GroupedInternal<>(grouped);
        groupedStores.put(copy.name(), copy);
    }

    public <K, V, S extends StateStore> void registerStore(Materialized<K, V, S> materialized) {
        var copy = new MaterializedInternal<>(materialized);
        stateStores.put(copy.storeName(), copy);
    }

    public Topology build() {
        return builder.build();
    }
}
