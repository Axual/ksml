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


import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.IgnoreParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.type.UserType;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.Functions;

public abstract class FunctionDefinitionParser<T extends FunctionDefinition> extends DefinitionParser<T> {
    private final boolean requireType;

    protected FunctionDefinitionParser(boolean requireType) {
        this.requireType = requireType;
    }

    protected StructsParser<T> parserWithStores(Class<T> resultClass, String type, String description, Constructor1<T, FunctionDefinition> constructor) {
        return parser(resultClass, type, description, true, (name, params, globalCode, code, expression, resultType, stores, tags) -> FunctionDefinition.as(type, name, params, globalCode, code, expression, resultType, stores), constructor);
    }

    protected StructsParser<T> parserWithoutStores(Class<T> resultClass, String type, String description, Constructor1<T, FunctionDefinition> constructor) {
        return parser(resultClass, type, description, false, (name, params, globalCode, code, expression, resultType, stores, tags) -> FunctionDefinition.as(type, name, params, globalCode, code, expression, resultType, null), constructor);
    }

    private StructsParser<T> parser(Class<T> resultClass, String type, String description, boolean includeStores, Constructor7<FunctionDefinition, String, List<ParameterDefinition>, String, String, String, UserType, List<String>> innerConstructor, Constructor1<T, FunctionDefinition> outerConstructor) {
        final var parseType = resultClass == FunctionDefinition.class;
        final var doc = "Defines a " + description + " function, that gets injected into the Kafka Streams topology";
        final var name = optional(stringField(Functions.NAME, "The name of the " + description + ". If this field is not defined, then the name is derived from the context."));
        final var params = optional(listField(Functions.PARAMETERS, "parameter", "parameter", "A list of parameters to be passed into the " + description, new ParameterDefinitionParser()));
        final var globalCode = optional(codeField(Functions.GLOBAL_CODE, "Global (multiline) code that gets loaded into the Python context outside of the " + description + ". Can be used for defining eg. global variables."));
        final var code = optional(codeField(Functions.CODE, "The (multiline) code of the " + description + "."));
        final var expression = optional(codeField(Functions.EXPRESSION, "The (multiline) expression returned by the " + description + ". Used as an alternative for 'return' statements in the code."));
        final var resultType = optional(userTypeField(Functions.RESULT_TYPE, "The data type returned by the " + description + ". Only required for function types, which are not pre-defined."));
        final var stores = includeStores
                ? optional(listField(Functions.STORES, "store-name", "store", "A list of store names that the " + description + " uses. Only required if the function wants to use a state store.", new StringValueParser()))
                : new IgnoreParser<List<String>>();
        // We assume that the resultClass is always either using stores or not using stores, but not a combination of both. Hence, we do not provide a definitionVariant extension to distinguish between the two.
        final var parser = structsParser(resultClass, parseType || requireType ? "" : KSMLDSL.Types.WITH_IMPLICIT_STORE_TYPE_POSTFIX, doc, name, params, globalCode, code, expression, resultType, stores, innerConstructor);
        return new StructsParser<>() {
            @Override
            public T parse(ParseNode node) {
                var rawFunction = parser.parse(node);
                if (rawFunction != null) {
                    if (rawFunction.name() == null) rawFunction = rawFunction.withName(node.longName());
                    if (rawFunction.globalCode().length > 0 || rawFunction.code().length > 0 || rawFunction.expression() != null) {
                        return outerConstructor.construct(rawFunction, node.tags());
                    }
                    return null;
                }
                return null;
            }

            @Override
            public List<StructSchema> schemas() {
                return parser.schemas();
            }
        };
    }
}
