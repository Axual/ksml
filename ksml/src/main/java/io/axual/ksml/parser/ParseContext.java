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


import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface ParseContext {
    String getNamePrefix();

    Map<String, BaseStreamDefinition> getStreamDefinitions();

    <T extends BaseStreamWrapper> T getStreamWrapper(BaseStreamDefinition definition, Class<T> resultClass);

    StreamWrapper getStreamWrapper(BaseStreamDefinition definition);

    Map<String, FunctionDefinition> getFunctionDefinitions();

    UserFunction getUserFunction(FunctionDefinition definition, String name);

    Map<String, StateStoreDefinition> getStoreDefinitions();

    Map<String, AtomicInteger> getTypeInstanceCounters();

    NotationLibrary getNotationLibrary();

    Set<String> getRegisteredTopics();

    void registerTopic(String topic);

    void registerStateStore(StateStoreDefinition store);

    void registerStateStoreAsCreated(StateStoreDefinition store);
}
