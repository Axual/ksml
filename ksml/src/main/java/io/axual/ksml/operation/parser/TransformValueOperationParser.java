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
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.TransformValueOperation;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

public class TransformValueOperationParser extends OperationParser<TransformValueOperation> {
    public TransformValueOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TRANSFORM_VALUE, resources);
    }

    @Override
    protected StructsParser<TransformValueOperation> parser() {
        return structsParser(
                TransformValueOperation.class,
                "",
                "Convert the value of every record in the stream to another value",
                operationNameField(),
                functionField(KSMLDSL.Operations.Transform.MAPPER, "A function that converts the value of every record into another value", new ValueTransformerDefinitionParser(false)),
                storeField(false, "Materialized view of the transformed table (only applies to tables, ignored for streams)", StoreType.KEYVALUE_STORE),
                (name, mapper, store, tags) -> new TransformValueOperation(storeOperationConfig(name, tags, store), mapper));
    }
}
