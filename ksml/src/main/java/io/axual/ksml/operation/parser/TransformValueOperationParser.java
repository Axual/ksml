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


import io.axual.ksml.definition.parser.ValueTransformerDefinitionParser;
import io.axual.ksml.operation.TransformValueOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TransformValueOperationParser extends StoreOperationParser<TransformValueOperation> {
    private final String name;

    protected TransformValueOperationParser(String name) {
        this.name = name;
    }

    @Override
    public TransformValueOperation parse(YamlNode node) {
        if (node == null) return null;
        return new TransformValueOperation(
                storeOperationConfig(name, node, MATERIALIZED_ATTRIBUTE),
                parseFunction(node, TRANSFORMVALUE_MAPPER_ATTRIBUTE, new ValueTransformerDefinitionParser()));
    }
}
