package io.axual.ksml.operation.parser;

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

import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.parser.StateStoreDefinitionParser;
import io.axual.ksml.operation.StoreOperation;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.YamlNode;

public abstract class StoreOperationParser<T extends StoreOperation> extends OperationParser<T> {
    protected StoreOperationParser(ParseContext context) {
        super(context);
    }

    protected StoreOperationConfig storeOperationConfig(String name, YamlNode node, String childName) {
        final var store = parseStoreInlineOrReference(node, childName, new StateStoreDefinitionParser());
        return new StoreOperationConfig(
                name,
                context.getNotationLibrary(),
                store,
                context::registerStateStoreAsCreated);
    }

    private StateStoreDefinition parseStoreInlineOrReference(YamlNode parent, String childName, BaseParser<StateStoreDefinition> parser) {
        return new ReferenceOrInlineParser<>("state store", childName, context.getStoreDefinitions()::get, parser).parseDefinition(parent);
    }
}
