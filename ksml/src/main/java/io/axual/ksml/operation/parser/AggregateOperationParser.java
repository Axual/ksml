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
import io.axual.ksml.definition.parser.InitializerDefinitionParser;
import io.axual.ksml.definition.parser.MergerDefinitionParser;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.AggregateOperation;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class AggregateOperationParser extends StoreOperationParser<AggregateOperation> {
    public AggregateOperationParser(TopologyResources resources) {
        super("aggregate", resources);
    }

    @Override
    protected StructParser<AggregateOperation> parser() {
        final var storeField = storeField(false, "Materialized view of the aggregation", StoreType.WINDOW_STORE);
        return structParser(
                AggregateOperation.class,
                "",
                "An aggregate operation",
                operationTypeField(Operations.AGGREGATE),
                operationNameField(),
                functionField(Operations.Aggregate.INITIALIZER, "The initializer function, which generates an initial value for every set of aggregated records", new InitializerDefinitionParser()),
                optional(functionField(Operations.Aggregate.AGGREGATOR, "(GroupedStream, SessionWindowedStream, TimeWindowedStream) The aggregator function, which combines a value with the previous aggregation result and outputs a new aggregation result", new AggregatorDefinitionParser())),
                optional(functionField(Operations.Aggregate.MERGER, "(SessionWindowedStream, SessionWindowedCogroupedStream) A function that combines two aggregation results", new MergerDefinitionParser())),
                optional(functionField(Operations.Aggregate.ADDER, "(GroupedTable) A function that adds a record to the aggregation result", new AggregatorDefinitionParser())),
                optional(functionField(Operations.Aggregate.SUBTRACTOR, "(GroupedTable) A function that removes a record from the aggregation result", new AggregatorDefinitionParser())),
                storeField,
                (type, name, init, aggr, merg, add, sub, store) -> new AggregateOperation(new StoreOperationConfig(namespace(), name, null, store), init, aggr, merg, add, sub));
    }
}
