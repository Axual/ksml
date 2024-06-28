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


import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.parser.ReducerDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ReduceOperation;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

import java.util.ArrayList;
import java.util.List;

public class ReduceOperationParser extends StoreOperationParser<ReduceOperation> {
    private static final String DOC = "Operation to reduce a series of records into a single aggregate result";
    private final StructsParser<ReduceOperation> reducerParser;
    private final StructsParser<ReduceOperation> addedSubtractorParser;
    private final List<StructSchema> schemas;

    public ReduceOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.REDUCE, resources);
        final var storeField = storeField(false, "Materialized view of the aggregation", StoreType.WINDOW_STORE);
        reducerParser = structsParser(
                ReduceOperation.class,
                "WithReducer",
                DOC,
                operationNameField(),
                functionField(KSMLDSL.Operations.Reduce.REDUCER, "A function that computes a new aggregate result", new ReducerDefinitionParser(false)),
                storeField,
                (name, reducer, store, tags) -> new ReduceOperation(storeOperationConfig(name, tags, store), reducer));
        addedSubtractorParser = structsParser(
                ReduceOperation.class,
                "WithAdderAndSubtractor",
                DOC,
                operationNameField(),
                functionField(KSMLDSL.Operations.Reduce.ADDER, "A function that adds a record to the aggregate result", new ReducerDefinitionParser(false)),
                functionField(KSMLDSL.Operations.Reduce.SUBTRACTOR, "A function that removes a record from the aggregate result", new ReducerDefinitionParser(false)),
                storeField,
                (name, add, sub, store, tags) -> new ReduceOperation(storeOperationConfig(name, tags, store), add, sub));
        schemas = new ArrayList<>();
        schemas.addAll(reducerParser.schemas());
        schemas.addAll(addedSubtractorParser.schemas());
    }

    @Override
    public StructsParser<ReduceOperation> parser() {
        return StructsParser.of(
                node -> {
                    final var result1 = reducerParser.parse(node);
                    if (result1 != null) return result1;
                    final var result2 = addedSubtractorParser.parse(node);
                    if (result2 != null) return result2;
                    throw new TopologyException("Error in reducer operation: " + node);
                },
                schemas);
    }
}
