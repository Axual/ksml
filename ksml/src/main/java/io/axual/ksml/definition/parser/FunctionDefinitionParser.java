package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class FunctionDefinitionParser extends BaseParser<FunctionDefinition> {
    public FunctionDefinitionParser() {
        super(value -> value ? "True" : "False");
    }

    @Override
    public FunctionDefinition parse(YamlNode node) {
        if (node == null) return null;
        return FunctionDefinition.as(
                new ListParser<>("function parameter", new ParameterDefinitionParser()).parse(node.get(FUNCTION_PARAMETERS_ATTRIBUTE)).toArray(new ParameterDefinition[0]),
                UserTypeParser.parse(parseString(node, FUNCTION_RESULTTYPE_ATTRIBUTE)),
                parseString(node, FUNCTION_EXPRESSION_ATTRIBUTE),
                parseMultilineText(node, FUNCTION_CODE_ATTRIBUTE),
                parseMultilineText(node, FUNCTION_GLOBALCODE_ATTRIBUTE),
                new ListParser<>("function store", new StringValueParser()).parse(node.get(FUNCTION_STORES_ATTRIBUTE)));
    }
}
