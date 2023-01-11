package io.axual.ksml.python;

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


import org.graalvm.polyglot.Value;

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.user.UserFunction;

public class PythonFunction extends UserFunction {
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper();
    private final Value function;

    public PythonFunction(PythonContext context, String name, FunctionDefinition definition) {
        super(name, definition.parameters, definition.resultType);
        function = context.registerFunction(name, definition);
    }

    @Override
    public DataObject call(DataObject... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (this.parameters.length != parameters.length) {
            throw new KSMLTopologyException("Parameter list does not match function spec: expected " + this.parameters.length + ", got " + parameters.length);
        }

        // Check all parameters and copy them into the interpreter as prefixed globals
        var arguments = convertParameters(parameters);

        try {
            // Call the prepared function
            Value pyResult = function.execute(arguments);

            if (pyResult.canExecute()) {
                throw new KSMLExecutionException("Python code results in a function instead of a value");
            }

            // Check if the function is supposed to return a result value
            if (resultType != null) {
                DataObject result = convertResult(pyResult);
                logCall(parameters, result);
                checkType(resultType.dataType(), result);
                return result;
            } else {
                logCall(parameters, null);
                return DataNull.INSTANCE;
            }
        } catch (Exception e) {
            logCall(parameters, null);
            throw FatalError.reportAndExit(new KSMLTopologyException("Error while executing function " + name + ": " + e.getMessage(), e));
        }
    }

    private Object[] convertParameters(DataObject... parameters) {
        Object[] result = new Object[parameters.length];
        for (var index = 0; index < parameters.length; index++) {
            checkType(this.parameters[index], parameters[index]);
            result[index] = MAPPER.fromDataObject(parameters[index]);
        }
        return result;
    }

    private DataObject convertResult(Value pyResult) {
        return MAPPER.toDataObject(resultType.dataType(), pyResult);
    }
}
