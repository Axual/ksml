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


import io.axual.ksml.definition.parser.ForEachActionDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.PeekOperation;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.StructParser;

public class PeekOperationParser extends OperationParser<PeekOperation> {
    public PeekOperationParser(TopologyResources resources) {
        super("peek", resources);
    }

    public StructParser<PeekOperation> parser() {
        return structParser(
                PeekOperation.class,
                "Operation to peek into a stream, without modifying the stream contents",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + KSMLDSL.Operations.PEEK + "\""),
                nameField(),
                functionField(KSMLDSL.Operations.FOR_EACH, true, "A function that gets called for every message in the stream", new ForEachActionDefinitionParser()),
                storeNamesField(),
                (type, name, action, stores) -> new PeekOperation(new StoreOperationConfig(namespace(), name, stores, null), action));
    }
}
