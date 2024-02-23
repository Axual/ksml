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


import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.StructParser;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.Functions;

public abstract class FunctionDefinitionParser<T extends FunctionDefinition> extends DefinitionParser<T> {
    private String defaultName;

    protected StructParser<T> parserWithStores(Class<T> resultClass, String functionType, String description, Constructor1<T, FunctionDefinition> constructor) {
        return parser(resultClass, functionType, description, (name, type, params, globalCode, code, expression, resultType, stores) -> FunctionDefinition.as(name, params, globalCode, code, expression, resultType, stores), constructor);
    }

    protected StructParser<T> parserWithoutStores(Class<T> resultClass, String functionType, String description, Constructor1<T, FunctionDefinition> constructor) {
        return parser(resultClass, functionType, description, (name, type, params, globalCode, code, expression, resultType, stores) -> FunctionDefinition.as(name, params, globalCode, code, expression, resultType, null), constructor);
    }

    private StructParser<T> parser(Class<T> resultClass, String functionType, String description, Constructor8<FunctionDefinition, String, String, List<ParameterDefinition>, String, String, String, UserType, List<String>> innerConstructor, Constructor1<T, FunctionDefinition> outerConstructor) {
        final var parseType = resultClass == FunctionDefinition.class;
        final var doc = "Defines a " + description + " function, that gets injected into the Kafka Streams topology";
        final var name = stringField(Functions.NAME, false, "The name of the " + description + ". If this field is not defined, then the name is derived from the context.");
        final var type = fixedStringField(Functions.TYPE, parseType, functionType, "The type of the function");
        final var params = listField(Functions.PARAMETERS, "function parameter", false, "A list of parameters to be passed into the " + description, new ParameterDefinitionParser());
        final var globalCode = codeField(Functions.GLOBAL_CODE, false, "Global (multiline) code that gets loaded into the Python context outside of the " + description + ". Can be used for defining eg. global variables.");
        final var code = codeField(Functions.CODE, false, "The (multiline) code of the " + description);
        final var expression = codeField(Functions.EXPRESSION, false, "The expression returned by the " + description);
        final var resultType = userTypeField(Functions.RESULT_TYPE, false, "The data type returned by the " + description);
        final var stores = listField(Functions.STORES, "state store name", false, "A list of store names that the " + description + " uses. Mandatory if the function wants to use a state store.", new StringValueParser());
        final var parser = structParser(resultClass, doc, name, type, params, globalCode, code, expression, resultType, stores, innerConstructor);
        return new StructParser<>() {
            @Override
            public T parse(ParseNode node) {
                var rawFunction = parser.parse(node);
                if (rawFunction != null) {
                    if (rawFunction.name() == null) rawFunction = rawFunction.withName(node.longName());
                    if (rawFunction.globalCode().length > 0 || rawFunction.code().length > 0 || rawFunction.expression() != null) {
                        return outerConstructor.construct(rawFunction);
                    }
                    return null;
                }
                return null;
            }

            @Override
            public StructSchema schema() {
                return parser.schema();
            }
        };
    }
}
