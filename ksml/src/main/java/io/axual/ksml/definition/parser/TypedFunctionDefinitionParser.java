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
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class TypedFunctionDefinitionParser extends BaseParser<FunctionDefinition> {
    @Override
    public FunctionDefinition parse(YamlNode node) {
        if (node == null) return null;

        final var type = parseString(node, FUNCTION_TYPE);
        final var parser = getParser(node, type);
        if (parser != null) {
            try {
                return parser.parse(node.appendName(type));
            } catch (RuntimeException e) {
                throw new KSMLParseException(node, "Error parsing typed function");
            }
        }

        return new FunctionDefinitionParser().parse(node.appendName("generic"));
    }

    private BaseParser<? extends FunctionDefinition> getParser(YamlNode node, String type) {
        if (type == null) return new FunctionDefinitionParser();
        return switch (type) {
            case FUNCTION_TYPE_AGGREGATOR -> new AggregatorDefinitionParser();
            case FUNCTION_TYPE_FOREACHACTION -> new ForEachActionDefinitionParser();
            case FUNCTION_TYPE_INITIALIZER -> new InitializerDefinitionParser();
            case FUNCTION_TYPE_KEYTRANSFORMER -> new KeyTransformerDefinitionParser();
            case FUNCTION_TYPE_KEYVALUETOKEYVALUELISTTRANSFORMER ->
                    new KeyValueToKeyValueListTransformerDefinitionParser();
            case FUNCTION_TYPE_KEYVALUETOVALUELISTTRANSFORMER -> new KeyValueToValueListTransformerDefinitionParser();
            case FUNCTION_TYPE_KEYVALUEMAPPER, FUNCTION_TYPE_KEYVALUETRANSFORMER ->
                    new KeyValueTransformerDefinitionParser();
            case FUNCTION_TYPE_MERGER -> new MergerDefinitionParser();
            case FUNCTION_TYPE_PREDICATE -> new PredicateDefinitionParser();
            case FUNCTION_TYPE_VALUEJOINER, FUNCTION_TYPE_REDUCER -> new ReducerDefinitionParser();
            case FUNCTION_TYPE_STREAMPARTITIONER -> new StreamPartitionerDefinitionParser();
            case FUNCTION_TYPE_TOPICNAMEEXTRACTOR -> new TopicNameExtractorDefinitionParser();
            case FUNCTION_TYPE_VALUETRANSFORMER -> new ValueTransformerDefinitionParser();
            case FUNCTION_TYPE_GENERIC -> new FunctionDefinitionParser();
            default -> throw FatalError.parseError(node, "Unknown function type: " + type);
        };
    }
}
