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
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.store.StoreType;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class AggregateOperationParser extends StoreOperationParser<AggregateOperation> {
    public AggregateOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public AggregateOperation parse(YamlNode node) {
        if (node == null) return null;
        return new AggregateOperation(
                storeOperationConfig(node, STORE_ATTRIBUTE, StoreType.WINDOW_STORE),
                parseOptionalFunction(node, AGGREGATE_INITIALIZER_ATTRIBUTE, new InitializerDefinitionParser()),
                parseOptionalFunction(node, AGGREGATE_AGGREGATOR_ATTRIBUTE, new AggregatorDefinitionParser()),
                parseOptionalFunction(node, AGGREGATE_MERGER_ATTRIBUTE, new MergerDefinitionParser()),
                parseOptionalFunction(node, AGGREGATE_ADDER_ATTRIBTUE, new AggregatorDefinitionParser()),
                parseOptionalFunction(node, AGGREGATE_SUBTRACTOR_ATTRIBUTE, new AggregatorDefinitionParser()));
    }
}
