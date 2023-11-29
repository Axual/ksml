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

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.exception.KSMLTopologyException;

import java.util.HashMap;
import java.util.Map;

public class TopologyResources {
    // All registered user functions
    private final Map<String, FunctionDefinition> functions = new HashMap<>();
    // All registered state stores
    private final Map<String, StateStoreDefinition> stateStores = new HashMap<>();
    // All registered KStreams, KTables and KGlobalTables
    private final Map<String, TopicDefinition> topics = new HashMap<>();

    public void register(String name, FunctionDefinition functionDefinition) {
        if (functions.containsKey(name)) {
            throw new KSMLTopologyException("Function definition must be unique: " + name);
        }
        if (functionDefinition.name != null && !name.equals(functionDefinition.name)) {
            throw new KSMLTopologyException("Function name inconsistently defined: " + name + " and " + functionDefinition.name);
        }
        functions.put(name, functionDefinition);
    }

    public FunctionDefinition function(String name) {
        return functions.get(name);
    }

    public Map<String, FunctionDefinition> functions() {
        return ImmutableMap.copyOf(functions);
    }

    public void register(String name, StateStoreDefinition store) {
        register(name, store, false);
    }

    public void register(String name, StateStoreDefinition store, boolean canBeDuplicate) {
        // Check if the store is properly named
        if (store.name() == null || store.name().isEmpty()) {
            throw new KSMLTopologyException("StateStore does not have a name: " + store);
        }
        // Check if the name is equal to the store name (otherwise a parsing error occurred)
        if (!store.name().equals(name)) {
            throw new KSMLTopologyException("StateStore name mistmatch: this is a parsing error: " + store);
        }
        // State stores can only be registered once. Duplicate names are a sign of faulty KSML definitions
        if (stateStores.containsKey(store.name())) {
            if (!canBeDuplicate) {
                throw new KSMLTopologyException("StateStore is not unique: " + store.name());
            }
            if (!stateStores.get(store.name()).equals(store)) {
                throw new KSMLTopologyException("StateStore definition conflicts earlier registration: " + store);
            }
        } else {
            stateStores.put(store.name(), store);
        }
    }

    public StateStoreDefinition stateStore(String name) {
        return stateStores.get(name);
    }

    public Map<String, StateStoreDefinition> stateStores() {
        return ImmutableMap.copyOf(stateStores);
    }

    public void register(String name, TopicDefinition def) {
        if (topics.containsKey(name)) {
            throw new KSMLTopologyException("Topic definition must be unique: " + name);
        }
        topics.put(name, def);
    }

    public TopicDefinition topic(String name) {
        return topics.get(name);
    }

    public Map<String, TopicDefinition> topics() {
        return ImmutableMap.copyOf(topics);
    }
}