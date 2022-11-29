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


import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.util.Arrays;

import io.axual.ksml.data.mapper.PythonDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.user.UserFunction;

public class PythonFunction extends UserFunction {
    private static final String PYTHON = "python";
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper();
    private static final Context CONTEXT = Context.newBuilder(PYTHON)
            .allowNativeAccess(true)
            .allowPolyglotAccess(PolyglotAccess.ALL)
            .allowHostAccess(HostAccess.ALL)
            .allowHostClassLookup(name -> name.equals("java.util.ArrayList") || name.equals("java.util.HashMap"))
            .build();
    private final Value function;

    public PythonFunction(String name, FunctionDefinition definition) {
        super(name, definition.parameters, definition.resultType);

        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(definition.code).map(line -> "  " + line).toArray(String[]::new);

        // Prepare a list of parameter names
        String[] params = Arrays.stream(parameters).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        final var globalCode = String.join("\n", definition.globalCode) + "\n";

        // prepare function (if any) and expression from the function definition
        final var functionAndExpression = "def " + name + "_function(" + String.join(",", params) + "):\n" +
                String.join("\n", functionCode) + "\n" +
                "  return" + (resultType != null ? " " + definition.expression : "") + "\n" +
                "\n";

        // prepare the actual caller for the code
        final var pyCallerCode = "def " + name + "_caller(" + String.join(",", params) + "):\n  " +
                "  return convert_from(" + name + "_function(" + String.join(",", params) + "))\n";

        final var pythonCodeTemplate = """
                import polyglot
                import java
                                
                ArrayList = java.type('java.util.ArrayList')
                HashMap = java.type('java.util.HashMap')
                                
                # global Python code goes here (first argument)
                %1$s
                                
                # function definition and expression go here (second argument)
                @polyglot.export_value
                %2$s
                                
                def convert_to(value):
                  print('In convert_to: ')
                  print(value)
                  return value
                  
                def convert_from(value):
                  if isinstance(value, (list, tuple)):
                    result = ArrayList()
                    for e in value:
                      result.add(convert_from(e))
                    return result
                  if type(value) is dict:
                    result = HashMap()
                    for k, v in value.items():
                      result.put(convert_from(k), convert_from(v))
                    return result
                  return value
                  
                # caller definition goes here (third argument)
                @polyglot.export_value
                %3$s  
                """;

        final var pyCode = pythonCodeTemplate.formatted(globalCode, functionAndExpression, pyCallerCode);
        Source script = Source.create(PYTHON, pyCode);
        CONTEXT.eval(script);
        function = CONTEXT.getPolyglotBindings().getMember(name + "_caller");
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
                return new DataNull();
            }
        } catch (Exception e) {
            logCall(parameters, null);
            throw new KSMLTopologyException("Error while executing function " + name + ": " + e.getMessage());
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
        // The converted result value from Python
        try {
            return MAPPER.toDataObject(resultType.dataType(), pyResult);
        } catch (KSMLExecutionException e) {
            // Ignore conversion error here
            return null;
        }
    }
}
