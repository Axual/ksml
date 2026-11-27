package io.axual.ksml.definition;

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


import io.axual.ksml.data.type.DataType;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.type.UserType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Getter
@EqualsAndHashCode
@SuppressWarnings("java:S2160") // Lombok writes equals and hashCode
public class FunctionDefinition extends AbstractDefinition {
    private static final String[] EMPTY_STRING_ARRAY = {};
    private static final ParameterDefinition[] EMPTY_PARAMETER_ARRAY = {};
    private static final List<String> EMPTY_STRING_LIST = new ArrayList<>();
    private final String type;
    private final String namespace;
    private final String name;
    private final ParameterDefinition[] parameters;
    private final String[] globalCode;
    private final String[] code;
    private final String[] expression;
    private final UserType resultType;
    private final List<InjectedParameter> injectedParameters;
    private final List<String> storeNames;

    private FunctionDefinition(String type, String namespace, String name, ParameterDefinition[] parameters, PythonSource source, UserType resultType, List<InjectedParameter> injectedParameters, List<String> storeNames) {
        this.type = type;
        this.namespace = namespace;
        this.name = name;
        this.parameters = parameters;
        this.resultType = resultType;
        this.expression = source != null && source.expression() != null ? source.expression().toArray(EMPTY_STRING_ARRAY) : null;
        this.code = source != null ? source.code().toArray(EMPTY_STRING_ARRAY) : EMPTY_STRING_ARRAY;
        this.globalCode = source != null ? source.globalCode().toArray(EMPTY_STRING_ARRAY) : EMPTY_STRING_ARRAY;
        this.injectedParameters = injectedParameters != null ? List.copyOf(injectedParameters) : List.of();
        this.storeNames = storeNames != null && !storeNames.isEmpty() ? storeNames : EMPTY_STRING_LIST;
    }

    protected FunctionDefinition(FunctionDefinition definition) {
        this.type = definition.type;
        this.namespace = definition.namespace;
        this.name = definition.name;
        this.parameters = definition.parameters;
        this.resultType = definition.resultType;
        this.expression = definition.expression;
        this.code = definition.code;
        this.globalCode = definition.globalCode;
        this.injectedParameters = definition.injectedParameters;
        this.storeNames = definition.storeNames != null ? definition.storeNames : EMPTY_STRING_LIST;
    }

    public static FunctionDefinition as(String type, String namespace, String name, List<ParameterDefinition> parameters, PythonSource source, UserType resultType, List<InjectedParameter> injectedParameters, List<String> storeNames) {
        return as(type, namespace, name, parameters != null ? parameters.toArray(EMPTY_PARAMETER_ARRAY) : EMPTY_PARAMETER_ARRAY, source, resultType, injectedParameters, storeNames);
    }

    public static FunctionDefinition as(String type, String namespace, String name, ParameterDefinition[] parameters, PythonSource source, UserType resultType, List<InjectedParameter> injectedParameters, List<String> storeNames) {
        return new FunctionDefinition(type, namespace, name, parameters, source, resultType, injectedParameters, storeNames);
    }

    // Add explicitly named parameters to the default set of parameters. The default parameters take precedence in the
    // ordering of all arguments.
    protected static ParameterDefinition[] mergeParameters(ParameterDefinition[] fixedParams, ParameterDefinition[] specifiedParams) {
        // First, create the set of all named parameters to get the required size of the result array
        final var paramNames = new HashSet<String>();
        Arrays.stream(fixedParams).forEach(p -> paramNames.add(p.name()));
        Arrays.stream(specifiedParams).forEach(p -> paramNames.add(p.name()));
        final var result = new ParameterDefinition[paramNames.size()];

        // From here on, the paramNames set becomes the set of parameters to still include in the result
        // The index value is the cursor into the result array that we fill below
        var index = 0;

        // Copy every required fixed parameter into the parameter array
        for (final var param : fixedParams) {
            if (!param.isOptional()) result[index++] = param;
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

        // Copy the optional fixed parameters last, so specified parameters keep their position for positional calls
        for (final var param : fixedParams) {
            if (param.isOptional()) result[index++] = param;
        }

        // Return the integrated parameter list
        return result;
    }

    public FunctionDefinition withType(String type) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withNamespace(String namespace) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withNamespaceIfNotSet(String namespace) {
        return new FunctionDefinition(type, this.namespace != null ? this.namespace : namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withName(String name) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withNameIfNotSet(String name) {
        return new FunctionDefinition(type, namespace, this.name != null ? this.name : name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withParameters(ParameterDefinition[] parameters) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withDefaultResultType(DataType defaultResultType) {
        return withDefaultResultType(new UserType(defaultResultType));
    }

    public FunctionDefinition withDefaultResultType(UserType defaultResultType) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType != null ? resultType : defaultResultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withResultType(UserType resultType) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withDefaultExpression(String expression) {
        return withDefaultExpression(new String[]{expression});
    }

    public FunctionDefinition withDefaultExpression(String[] defaultExpression) {
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression != null && !Arrays.equals(EMPTY_STRING_ARRAY, expression) ? expression : defaultExpression), resultType, injectedParameters, storeNames);
    }

    public FunctionDefinition withInjectedParameter(InjectedParameter variable) {
        final var newVars = new ArrayList<>(injectedParameters);
        if (injectedParameters.stream().noneMatch(existing -> existing.name().equals(variable.name())))
            newVars.add(variable);
        return new FunctionDefinition(type, namespace, name, parameters, PythonSource.of(globalCode, code, expression), resultType, newVars, storeNames);
    }

    public FunctionDefinition validateNoResultTypeDefined() {
        if (resultType != null)
            throw functionResultError("Function type '" + type + "' can not return a result");
        return this;
    }

    public FunctionDefinition validateResultTypeDefined() {
        if (resultType == null)
            throw functionResultError("Function type '" + type + "' requires an explicit resultType");
        return this;
    }

    protected TopologyException functionResultError(String message) {
        throw new TopologyException(message + ": function=" + name() + ", type=" + type() + ", resultType=" + resultType());
    }

    // Count the number of fixed parameters. Throw an error if the ordering is illegal (ie. fixed parameters should
    // always come before optional parameters in the params list)
    public int fixedParameterCount() {
        var inOptionals = false;
        var fixedParamCount = 0;
        for (final var param : parameters) {
            if (!param.isOptional()) {
                if (inOptionals) {
                    throw new TopologyException("Error in parameter list of %s.%s, fixed parameters should be listed first: %s".formatted(namespace, name, Arrays.toString(parameters)));
                }
                fixedParamCount++;
            } else {
                inOptionals = true;
            }
        }
        return fixedParamCount;
    }
}
