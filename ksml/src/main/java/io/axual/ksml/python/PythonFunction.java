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


import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.util.Arrays;

import io.axual.ksml.data.NativeTypeConverter;
import io.axual.ksml.data.PythonTypeConverter;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.util.StringUtil;

public class PythonFunction extends UserFunction {
    protected final PythonInterpreter interpreter;
    private final NativeTypeConverter toNative = new NativeTypeConverter();
    private final PythonTypeConverter toPython = new PythonTypeConverter();

    public PythonFunction(PythonInterpreter interpreter, String name, FunctionDefinition definition) {
        super(name, definition.parameters, definition.resultType);
        this.interpreter = interpreter;
        prepare(definition.globalCode, definition.code, definition.expression);
    }

    protected void prepare(String[] globalCode, String[] code, String resultExpression) {
        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(code).map(line -> "  " + line).toArray(String[]::new);

        // Prepare a list of parameter names
        String[] params = Arrays.stream(parameters).map(p -> p.name).toArray(String[]::new);

        // Prepare the Python code to load
        String pyCode = StringUtil.join("\n", globalCode) + "\n" +
                "def " + name + "_function(" + StringUtil.join(",", params) + "):\n" +
                StringUtil.join("\n", functionCode) + "\n" +
                "  return" + (resultType != null ? " " + resultExpression : "") + "\n";

        // Load the function and its global code
        interpreter.exec(pyCode);
    }

    @Override
    public Object call(Object... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (this.parameters.length != parameters.length) {
            throw new KSMLTopologyException("Parameter list does not match function spec: expected " + this.parameters.length + ", got " + parameters.length);
        }

        // Check all parameters and copy them into the interpreter as prefixed globals
        for (var index = 0; index < parameters.length; index++) {
            checkType(this.parameters[index], parameters[index]);
            interpreter.set(name + "_" + this.parameters[index].name, toPython.convert(parameters[index]));
        }

        // Create a list of prefixed parameter names to pass to the function
        String[] params = Arrays.stream(this.parameters).map(p -> name + "_" + p.name).toArray(String[]::new);

        try {
            // Call the prepared function
            PyObject pyResult = interpreter.eval(name + "_" + "function(" + StringUtil.join(",", params) + ")");

            // Process result
            if (resultType != null) {
                if (pyResult == null) {
                    throw new KSMLTopologyException("Illegal return from function: null");
                }

                Object result = toNative.convert(pyResult);
                logCall(parameters, result);
                checkType(result, resultType);
                return result;
            } else {
                logCall(parameters, null);
                return null;
            }
        } catch (Exception e) {
            logCall(parameters, null);
            throw new KSMLTopologyException("Error while executing function " + name + ": " + e.getMessage());
        }
    }
}
