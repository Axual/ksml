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


import io.axual.ksml.definition.parser.KeyTransformerDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.TransformKeyOperation;
import io.axual.ksml.parser.StructsParser;

public class TransformKeyOperationParser extends OperationParser<TransformKeyOperation> {
    public TransformKeyOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TRANSFORM_KEY, resources);
    }

    @Override
    protected StructsParser<TransformKeyOperation> parser() {
        return structsParser(
                TransformKeyOperation.class,
                "",
                "Convert the key of every record in the stream to another key",
                operationNameField(),
                functionField(KSMLDSL.Operations.Transform.MAPPER, "A function that computes a new key for each record", new KeyTransformerDefinitionParser(false)),
                storeNamesField(),
                (name, mapper, storeNames, tags) -> new TransformKeyOperation(operationConfig(name, tags, storeNames), mapper));
    }
}
