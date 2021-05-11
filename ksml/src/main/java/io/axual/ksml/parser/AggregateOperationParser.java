package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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



import io.axual.ksml.operation.AggregateOperation;

import static io.axual.ksml.dsl.KSMLDSL.AGGREGATE_ADDER_ATTRIBTUE;
import static io.axual.ksml.dsl.KSMLDSL.AGGREGATE_AGGREGATOR_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.AGGREGATE_INITIALIZER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.AGGREGATE_MERGER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.AGGREGATE_SUBTRACTOR_ATTRIBUTE;

public class AggregateOperationParser extends ContextAwareParser<AggregateOperation> {

    protected AggregateOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public AggregateOperation parse(YamlNode node) {
        if (node == null) return null;
        return new AggregateOperation(
                parseFunction(node, AGGREGATE_INITIALIZER_ATTRIBUTE, new InitializerDefinitionParser()),
                parseFunction(node, AGGREGATE_AGGREGATOR_ATTRIBUTE, new AggregatorDefinitionParser()),
                parseFunction(node, AGGREGATE_MERGER_ATTRIBUTE, new MergerDefinitionParser()),
                parseFunction(node, AGGREGATE_ADDER_ATTRIBTUE, new AggregatorDefinitionParser()),
                parseFunction(node, AGGREGATE_SUBTRACTOR_ATTRIBUTE, new AggregatorDefinitionParser()));
    }
}
