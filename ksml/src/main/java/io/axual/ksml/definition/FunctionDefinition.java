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


import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.type.DataType;

public class FunctionDefinition {
    public final ParameterDefinition[] parameters;
    public final DataType resultType;
    public final String expression;
    public final String[] code;
    public final String[] globalCode;

    public static FunctionDefinition as(ParameterDefinition[] parameters, DataType resultType, String expression, String[] code, String[] globalCode) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode);
    }

    public FunctionDefinition withCode(String expression, String[] code, String[] globalCode) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode);
    }

    public FunctionDefinition withParameters(ParameterDefinition[] parameters) {
        return new FunctionDefinition(parameters, resultType, expression, code, globalCode);
    }

    public FunctionDefinition withResult(DataType resultType) {
        return new FunctionDefinition(parameters, resultType, resultType != null ? expression : "", code, globalCode);
    }

    private FunctionDefinition(ParameterDefinition[] parameters, DataType resultType, String expression, String[] code, String[] globalCode) {
        this.parameters = parameters;
        this.resultType = resultType;
        this.expression = expression;
        this.code = code != null ? code : new String[]{};
        this.globalCode = globalCode != null ? globalCode : new String[]{};
    }

    protected FunctionDefinition(FunctionDefinition definition) {
        this.parameters = definition.parameters;
        this.resultType = definition.resultType;
        this.expression = definition.expression;
        this.code = definition.code != null ? definition.code : new String[]{};
        this.globalCode = definition.globalCode != null ? definition.globalCode : new String[]{};
    }

    // Check if parameters were specified already. If so, then use the explicitly defined parameters. If not, use the default ones.
    protected static ParameterDefinition[] getParameters(ParameterDefinition[] specified, ParameterDefinition[] defaultParams) {
        if (specified == null || specified.length == 0) {
            return defaultParams;
        }
        if (specified.length != defaultParams.length) {
            throw new KSMLApplyException("Specified parameter list does not contain " + defaultParams.length + " parameters");
        }
        return specified;
    }
}
