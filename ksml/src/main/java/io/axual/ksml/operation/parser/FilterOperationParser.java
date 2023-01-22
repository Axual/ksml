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


import io.axual.ksml.definition.parser.PredicateDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.operation.FilterOperation;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.user.UserFunction;

import static io.axual.ksml.dsl.KSMLDSL.FILTER_PREDICATE_ATTRIBUTE;

public class FilterOperationParser extends OperationParser<FilterOperation> {
    private final String name;

    protected FilterOperationParser(String name, ParseContext context) {
        super(context);
        this.name = name;
    }

    @Override
    public FilterOperation parse(YamlNode node) {
        if (node == null) return null;
        UserFunction predicate = parseFunction(node, FILTER_PREDICATE_ATTRIBUTE, new PredicateDefinitionParser());
        if (predicate != null) {
            return new FilterOperation(
                    operationConfig(name),
                    predicate);
        }
        throw new KSMLParseException(node, "Predicate not specified or function unknown");
    }
}
