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


import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.definition.parser.PredicateDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.FilterNotOperation;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

public class FilterNotOperationParser extends StoreOperationParser<FilterNotOperation> {
    public FilterNotOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.FILTER_NOT, resources);
    }

    public StructParser<FilterNotOperation> parser() {
        return structParser(
                FilterNotOperation.class,
                "",
                "Filter records based on the inverse result of a predicate function",
                operationTypeField(),
                operationNameField(),
                functionField(KSMLDSL.Operations.Filter.PREDICATE, "A function that returns \"false\" when records are accepted, \"true\" otherwise", new PredicateDefinitionParser()),
                storeField(false, "Materialized view of the filtered table (only applies to tables, ignored for streams)", StoreType.KEYVALUE_STORE),
                storeNamesField(),
                (type, name, pred, store, stores, tags) -> {
                    if (pred != null)
                        return new FilterNotOperation(storeOperationConfig(name, tags, store, stores), pred);
                    throw new ExecutionException("Predicate not defined for " + type + " operation");
                });
    }
}
