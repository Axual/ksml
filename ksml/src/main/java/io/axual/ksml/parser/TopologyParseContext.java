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



import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.python.util.PythonInterpreter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.axual.ksml.dsl.BaseStreamDefinition;
import io.axual.ksml.dsl.FunctionDefinition;
import io.axual.ksml.dsl.GlobalTableDefinition;
import io.axual.ksml.dsl.StreamDefinition;
import io.axual.ksml.dsl.TableDefinition;
import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.SerdeGenerator;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

public class TopologyParseContext implements ParseContext {
    private final StreamsBuilder builder;
    private final PythonInterpreter interpreter;
    private final SerdeGenerator serdeGenerator;
    private final Map<String, BaseStreamDefinition> streamDefinitions;
    private final Map<String, FunctionDefinition> functionDefinitions;
    private final Map<String, StreamWrapper> streams = new HashMap<>();
    private final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();

    public TopologyParseContext(StreamsBuilder builder, PythonInterpreter interpreter, SerdeGenerator serdeGenerator, Map<String, BaseStreamDefinition> streamDefinitions, Map<String, FunctionDefinition> functionDefinitions) {
        this.builder = builder;
        this.interpreter = interpreter;
        this.serdeGenerator = serdeGenerator;
        this.streamDefinitions = streamDefinitions;
        this.functionDefinitions = functionDefinitions;
        streamDefinitions.forEach((name, def) -> streams.put(def.name, def.addToBuilder(builder, serdeGenerator)));
    }

    @Override
    public Map<String, BaseStreamDefinition> getStreams() {
        return streamDefinitions;
    }

    @Override
    public <T extends BaseStreamWrapper> T getStream(BaseStreamDefinition definition, Class<T> resultClass) {
        StreamWrapper result = streams.get(definition.name);
        if (result == null) {
            result = definition.addToBuilder(builder, serdeGenerator);
            streams.put(definition.name, result);
        }
        if (!resultClass.isInstance(result)) {
            throw new KSMLTopologyException("Stream is of incorrect type " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
        }
        return (T) result;
    }

    @Override
    public BaseStreamWrapper getStream(BaseStreamDefinition definition) {
        return getStream(definition, BaseStreamWrapper.class);
    }

    @Override
    public Map<String, FunctionDefinition> getFunctions() {
        return functionDefinitions;
    }

    @Override
    public UserFunction getFunction(FunctionDefinition definition, String name) {
        final PythonInterpreter functionInterpreter = interpreter != null ? interpreter : new PythonInterpreter();
        return new PythonFunction(functionInterpreter, name, definition);
    }

    @Override
    public Map<String, AtomicInteger> getTypeInstanceCounters() {
        return typeInstanceCounters;
    }

    public Topology build() {
        return builder.build();
    }
}
