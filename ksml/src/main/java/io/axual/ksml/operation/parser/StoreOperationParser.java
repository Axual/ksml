package io.axual.ksml.operation.parser;

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

import io.axual.ksml.data.parser.NamedObjectParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.parser.StateStoreDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.StoreOperation;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.store.StoreType;

import java.util.List;

public abstract class StoreOperationParser<T extends StoreOperation> extends OperationParser<T> {
    public StoreOperationParser(String type, TopologyResources resources) {
        super(type, resources);
    }

    protected StoreOperationConfig storeOperationConfig(String name, List<String> storeNames, StateStoreDefinition store) {
        return new StoreOperationConfig(resources().namespace(), name, storeNames, store);
    }

    protected StructParser<StateStoreDefinition> storeField(boolean mandatory, String doc, StoreType expectedStoreType) {
        final var stateStoreParser = new StateStoreDefinitionParser(expectedStoreType);
        final var resourceParser = new TopologyResourceParser<>("state store", KSMLDSL.Operations.STORE_ATTRIBUTE, doc, resources()::stateStore, stateStoreParser);
        final var schema = mandatory ? resourceParser.schema() : optional(resourceParser).schema();
        return new StructParser<>() {
            @Override
            public StateStoreDefinition parse(ParseNode node) {
                if (stateStoreParser instanceof NamedObjectParser nop)
                    nop.defaultName(node.longName());
                final var resource = resourceParser.parse(node);
                if (resource != null && resource.definition() instanceof StateStoreDefinition def) return def;
                if (!mandatory) return null;
                throw FatalError.parseError(node, "Mandatory state store not defined");
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
