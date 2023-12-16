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


import io.axual.ksml.definition.parser.AggregatorDefinitionParser;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.CogroupOperation;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class CogroupOperationParser extends StoreOperationParser<CogroupOperation> {
    public CogroupOperationParser(TopologyResources resources) {
        super("cogroup", resources);
    }

    @Override
    protected StructParser<CogroupOperation> parser() {
        final var storeField = storeField(false, "Materialized view of the cogroup", StoreType.WINDOW_STORE);
        return structParser(
                CogroupOperation.class,
                "A cogroup operation",
                operationTypeField(Operations.COGROUP),
                nameField(),
                functionField(Operations.Aggregate.AGGREGATOR, "(GroupedStream, SessionWindowedStream, TimeWindowedStream) The aggregator function, which combines a value with the previous aggregation result and outputs a new aggregation result", new AggregatorDefinitionParser()),
                storeField,
                (type, name, aggr, store) -> new CogroupOperation(new StoreOperationConfig(namespace(), name, null, store), aggr));
    }
}
