package io.axual.ksml.producer.parser;

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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.parser.FunctionDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.FUNCTION_TYPE;
import static io.axual.ksml.producer.dsl.ProducerDSL.FUNCTION_TYPE_GENERATOR;

public class TypedFunctionDefinitionParser extends BaseParser<FunctionDefinition> {
    @Override
    public FunctionDefinition parse(YamlNode node) {
        if (node == null) return null;

        final String type = parseString(node, FUNCTION_TYPE);
        if (type == null) {
            throw new KSMLParseException(node, "Type unspecified");
        }

        BaseParser<? extends FunctionDefinition> parser = getParser(type);
        if (parser != null) {
            try {
                return parser.parse(node.appendName(type));
            } catch (RuntimeException e) {
                throw new KSMLParseException(node, "Error parsing typed function");
            }
        }

        return new FunctionDefinitionParser().parse(node.appendName("generic"));
    }

    private BaseParser<? extends FunctionDefinition> getParser(String type) {
        return switch (type) {
            case FUNCTION_TYPE_GENERATOR -> new GeneratorDefinitionParser();
            default -> null;
        };
    }
}
