package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class ParameterDefinitionParser extends BaseParser<ParameterDefinition> {
    @Override
    public ParameterDefinition parse(YamlNode node) {
        if (node == null) return null;
        return new ParameterDefinition(
                parseString(node, FUNCTION_PARAMETER_NAME),
                UserTypeParser.parse(parseString(node, FUNCTION_PARAMETER_TYPE)).dataType(),
                true,
                parseString(node, FUNCTION_PARAMETER_DEFAULT_VALUE));
    }
}
