package io.axual.ksml.operation.parser;

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

import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public abstract class OperationParser<T extends StreamOperation> extends ContextAwareParser<T> {
    private static final String[] TEMPLATE = new String[0];

    protected OperationParser(ParseContext context) {
        super(context);
    }

    protected OperationConfig parseConfig(YamlNode node, String operationName) {
        var storeNames = new ListParser<>("operation store", new StringValueParser()).parse(node.get(OPERATION_STORES_ATTRIBUTE));
        return new OperationConfig(
                operationName,
                context.getNotationLibrary(),
                storeNames != null ? storeNames.toArray(TEMPLATE) : null);
    }
}
