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


import io.axual.ksml.definition.parser.KeyValueTransformerDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.operation.TransformKeyValueOperation;
import io.axual.ksml.parser.StructParser;

public class TransformKeyValueOperationParser extends OperationParser<TransformKeyValueOperation> {
    public TransformKeyValueOperationParser(TopologyResources resources) {
        super("transformKey", resources);
    }

    @Override
    protected StructParser<TransformKeyValueOperation> parser() {
        return structParser(
                TransformKeyValueOperation.class,
                "Convert the key/value of every record in the stream to another key/value",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + KSMLDSL.Operations.TRANSFORM_KEY_VALUE + "\""),
                nameField(),
                functionField(KSMLDSL.Operations.Transform.MAPPER, "A function that computes a new key/value for each record", new KeyValueTransformerDefinitionParser()),
                storeNamesField(),
                (type, name, mapper, storeNames) -> new TransformKeyValueOperation(new StoreOperationConfig(namespace(), name, storeNames, null), mapper));
    }
}
