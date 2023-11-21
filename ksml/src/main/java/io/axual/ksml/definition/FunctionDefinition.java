package io.axual.ksml.definition;

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


import io.axual.ksml.data.type.UserType;
import io.axual.ksml.exception.KSMLTopologyException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class FunctionDefinition {
    private static final String DEFINITION_LITERAL = "Definition";
    private static final String[] EMPTY_STRING_ARRAY = new String[]{};
    private static final List<String> EMPTY_STRING_LIST = new ArrayList<>();
    public final ParameterDefinition[] parameters;
    public final UserType resultType;
    public final String expression;
    public final String[] code;
    public final String[] globalCode;
    public final List<String> storeNames;

    public static FunctionDefinition as(ParameterDefinition[] parameters, UserType result, String expression, String[] code, String[] globalCode, List<String> storeNames) {
        return new FunctionDefinition(parameters, result, expression, code, globalCode, storeNames);
    }

    public FunctionDefinition withCode(String expression, String[] code, String[] globalCode, List<String> storeNames) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode, storeNames);
    }

    public FunctionDefinition withParameters(ParameterDefinition[] parameters) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode, storeNames);
    }

    public FunctionDefinition withoutResult() {
        return withResult(null);
    }

    public FunctionDefinition withResult(UserType resultType) {
        return new FunctionDefinition(parameters, resultType, resultType != null ? expression : null, code, globalCode, storeNames);
    }

    public FunctionDefinition withAResult() {
        var type = getClass().getSimpleName();
        if (DEFINITION_LITERAL.equals(type.substring(type.length() - DEFINITION_LITERAL.length()))) {
            type = type.substring(0, type.length() - DEFINITION_LITERAL.length());
        }
        return withAResult(type);
    }

    public FunctionDefinition withAResult(String functionType) {
        if (resultType == null)
            throw new KSMLTopologyException("Function type requires a result: " + functionType);
        return this;
    }

    public FunctionDefinition withStoreNames(List<String> storeNames) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode, storeNames);
    }

    private FunctionDefinition(ParameterDefinition[] parameters, UserType resultType, String expression, String[] code, String[] globalCode, List<String> storeNames) {
        this.parameters = parameters;
        this.resultType = resultType;
        this.expression = expression;
        this.code = code != null ? code : EMPTY_STRING_ARRAY;
        this.globalCode = globalCode != null ? globalCode : EMPTY_STRING_ARRAY;
        this.storeNames = storeNames != null ? storeNames : EMPTY_STRING_LIST;
    }

    protected FunctionDefinition(FunctionDefinition definition) {
        this.parameters = definition.parameters;
        this.resultType = definition.resultType;
        this.expression = definition.expression;
        this.code = definition.code != null ? definition.code : new String[]{};
        this.globalCode = definition.globalCode != null ? definition.globalCode : EMPTY_STRING_ARRAY;
        this.storeNames = definition.storeNames != null ? definition.storeNames : EMPTY_STRING_LIST;
    }

    // Add explicitly named parameters to the default set of parameters. The default parameters take precedence in the
    // ordering of all arguments.
    protected static ParameterDefinition[] mergeParameters(ParameterDefinition[] fixedParams, ParameterDefinition[] specifiedParams) {
        // First create the set of all named parameters to get the required size of the result array
        final var paramNames = new HashSet<String>();
        Arrays.stream(fixedParams).forEach(p -> paramNames.add(p.name()));
        Arrays.stream(specifiedParams).forEach(p -> paramNames.add(p.name()));
        final var result = new ParameterDefinition[paramNames.size()];

        // From here on, the paramNames set becomes the set of parameters to still include in the result
        // The index value is the cursor into the result array that we fill below
        var index = 0;

        // Copy every fixed parameter into the parameter array
        for (final var param : fixedParams) {
            result[index++] = param;
            // Remove from the list of "to do parameters"
            paramNames.remove(param.name());
        }

        // Copy every specified parameter into the result, unless it was included above as a fixed parameter
        for (final var param : specifiedParams) {
            // Only take non-fixed parameters into account
            if (paramNames.contains(param.name())) {
                result[index++] = param;
                // Remove param name to prevent that doubly specified parameters lead to array overflows
                paramNames.remove(param.name());
            }
        }

        // Return the integrated parameter list
        return result;
    }
}
