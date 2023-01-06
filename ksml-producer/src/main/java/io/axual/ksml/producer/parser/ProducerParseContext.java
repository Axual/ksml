package io.axual.ksml.producer.parser;

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


import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.user.UserFunction;

/**
 * Parse context
 */
public class ProducerParseContext implements ParseContext {
    private final PythonContext pythonContext = new PythonContext();
    private final NotationLibrary notationLibrary;
    private final Map<String, StreamDefinition> streamDefinitions = new HashMap<>();
    private final Map<String, FunctionDefinition> functionDefinitions = new HashMap<>();

    public ProducerParseContext(NotationLibrary notationLibrary) {
        this.notationLibrary = notationLibrary;
    }

    public void registerStreamDefinition(String name, StreamDefinition def) {
        if (streamDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Stream definition must be unique: " + name);
        }
        streamDefinitions.put(name, def);
    }

    public void registerFunction(String name, FunctionDefinition functionDefinition) {
        if (functionDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Function definition must be unique: " + name);
        }
        functionDefinitions.put(name, functionDefinition);
    }

    @Override
    public Map<String, StreamDefinition> getStreamDefinitions() {
        return streamDefinitions;
    }

    @Override
    public Map<String, FunctionDefinition> getFunctionDefinitions() {
        return functionDefinitions;
    }

    @Override
    public UserFunction getUserFunction(FunctionDefinition definition, String name) {
        return new PythonFunction(pythonContext, name, definition);
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }
}
