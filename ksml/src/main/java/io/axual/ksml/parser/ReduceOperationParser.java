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



import io.axual.ksml.operation.ReduceOperation;

import static io.axual.ksml.dsl.KSMLDSL.REDUCE_ADDER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.REDUCE_REDUCER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.REDUCE_SUBTRACTOR_ATTRIBUTE;

public class ReduceOperationParser extends ContextAwareParser<ReduceOperation> {
    protected ReduceOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public ReduceOperation parse(YamlNode node) {
        if (node == null) return null;
        return new ReduceOperation(
                parseFunction(node, REDUCE_REDUCER_ATTRIBUTE, new ReducerDefinitionParser()),
                parseFunction(node, REDUCE_ADDER_ATTRIBUTE, new ReducerDefinitionParser()),
                parseFunction(node, REDUCE_SUBTRACTOR_ATTRIBUTE, new ReducerDefinitionParser()));
    }
}
