package io.axual.ksml.python;

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


import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.streams.processor.StateStore;
import org.graalvm.polyglot.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.axual.ksml.data.type.UserType.DEFAULT_NOTATION;

public class PythonFunction extends UserFunction {
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper();
    private static final Map<String, StateStore> EMPTY_STORES = new HashMap<>();
    private static final LoggerBridge loggerBridge = new LoggerBridge();
    private static final String QUOTE = "\"";
    private final DataObjectConverter converter;
    private final Value function;

    public static PythonFunction fromAnon(PythonContext context, String name, FunctionDefinition definition, String loggerName) {
        return new PythonFunction(context, name, definition, loggerName);
    }

    public static PythonFunction fromNamed(PythonContext context, String name, FunctionDefinition definition) {
        return new PythonFunction(context, name, definition, "ksml.functions." + name);
    }

    private PythonFunction(PythonContext context, String name, FunctionDefinition definition, String loggerName) {
        super(name, definition.parameters, definition.resultType, definition.storeNames);
        converter = context.getConverter();
        final var pyCode = generatePythonCode(name, loggerName, definition);
        function = context.registerFunction(pyCode, name + "_caller");
        if (function == null) {
            System.out.println("Error in generated Python code:\n" + pyCode);
            throw FatalError.executionError("Error in function: " + name);
        }
    }

    @Override
    public DataObject call(StateStores stores, DataObject... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (this.fixedParameterCount > parameters.length) {
            throw new KSMLTopologyException("Parameter list does not match function spec: minimally expected " + this.parameters.length + ", got " + parameters.length);
        }
        if (this.parameters.length < parameters.length) {
            throw new KSMLTopologyException("Parameter list does not match function spec: maximally expected " + this.parameters.length + ", got " + parameters.length);
        }

        // Check all parameters and copy them into the interpreter as prefixed globals
        var globalVars = new HashMap<String, Object>();
        globalVars.put("loggerBridge", loggerBridge);
        globalVars.put("stores", stores != null ? stores : EMPTY_STORES);
        var arguments = convertParameters(globalVars, parameters);

        try {
            // Call the prepared function
            Value pyResult = function.execute(arguments);

            if (pyResult.canExecute()) {
                throw new KSMLExecutionException("Python code results in a function instead of a value");
            }

            // Check if the function is supposed to return a result value
            if (appliedResultType != null) {
                DataObject result = convertResult(pyResult);
                logCall(parameters, result);
                result = converter != null ? converter.convert(DEFAULT_NOTATION, result, appliedResultType) : result;
                checkType(appliedResultType.dataType(), result);
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

    private Object[] convertParameters(Map<String, Object> globalVariables, DataObject... parameters) {
        Object[] result = new Object[parameters.length + 1];
        result[0] = globalVariables;
        for (var index = 0; index < parameters.length; index++) {
            checkType(this.parameters[index], parameters[index]);
            result[index + 1] = MAPPER.fromDataObject(parameters[index]);
        }
        return result;
    }

    private DataObject convertResult(Value pyResult) {
        return MAPPER.toDataObject(appliedResultType.dataType(), pyResult);
    }

    private String generatePythonCode(String name, String loggerName, FunctionDefinition definition) {
        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(definition.code).map(line -> "  " + line).toArray(String[]::new);

        // Prepare a list of parameters for the function definition
        String[] defParams = Arrays.stream(definition.parameters).map(p -> p.name() + (p.isOptional() ? "=None" : "")).toArray(String[]::new);
        // Prepare a list of parameters for the function calling
        String[] callParams = Arrays.stream(definition.parameters).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        final var globalCode = String.join("\n", definition.globalCode) + "\n";

        // Code to include all global variables
        final var assignStores = definition.storeNames.stream()
                .map(storeName -> "  " + storeName + " = stores[\"" + storeName + "\"]\n")
                .collect(Collectors.joining());
        // Code to copy / initialize all global variables
        final var includeGlobals =
                "  global stores\n" +
                        "  global loggerBridge\n";
        final var initializeGlobals =
                "  stores = convert_to_python(globalVars[\"stores\"])\n" +
                        "  loggerBridge = globalVars[\"loggerBridge\"]\n";
        // Code to initialize optional parameters with default values
        final var initializeOptionalParams = Arrays.stream(definition.parameters)
                .filter(ParameterDefinition::isOptional)
                .filter(p -> p.defaultValue() != null)
                .map(p -> "  if " + p.name() + " == None:\n    " + p.name() + " = " + (p.type() == DataString.DATATYPE ? QUOTE : "") + p.defaultValue() + (p.type() == DataString.DATATYPE ? QUOTE : "") + "\n")
                .collect(Collectors.joining());

        // Prepare function (if any) and expression from the function definition
        final var functionAndExpression = "def " + name + "(" + String.join(",", defParams) + "):\n" +
                includeGlobals +
                "  log = loggerBridge.getLogger(\"" + loggerName + "\")\n" +
                assignStores +
                initializeOptionalParams +
                String.join("\n", functionCode) + "\n" +
                "  return" + (definition.resultType != null && definition.resultType.dataType() != DataNull.DATATYPE ? " " + definition.expression : "") + "\n" +
                "\n";

        // Prepare the actual caller for the code
        final var convertedParams = Arrays.stream(callParams).map(p -> "convert_to_python(" + p + ")").toList();
        final var pyCallerCode = "def " + name + "_caller(globalVars," + String.join(",", defParams) + "):\n" +
                includeGlobals +
                initializeGlobals +
                "  return convert_from_python(" + name + "(" + String.join(",", convertedParams) + "))\n";

        final var pythonCodeTemplate = """
                import polyglot
                import java
                                
                ArrayList = java.type('java.util.ArrayList')
                HashMap = java.type('java.util.HashMap')
                TreeMap = java.type('java.util.TreeMap')
                loggerBridge = None
                stores = None
                                
                # global Python code goes here (first argument)
                %1$s
                                
                # function definition and expression go here (second argument)
                @polyglot.export_value
                %2$s
                                
                def convert_to_python(value):
                  if value == None:
                    return None
                  if isinstance(value, (HashMap, TreeMap)):
                    result = dict()
                    for k, v in value.entrySet():
                      result[convert_to_python(k)] = convert_to_python(v)
                    return result
                  if isinstance(value, ArrayList):
                    result = []
                    for e in value:
                      result.append(convert_to_python(e))
                    return result
                  return value
                  
                def convert_from_python(value):
                  if value == None:
                    return None
                  if isinstance(value, (list, tuple)):
                    result = ArrayList()
                    for e in value:
                      result.add(convert_from_python(e))
                    return result
                  if type(value) is dict:
                    result = HashMap()
                    for k, v in value.items():
                      result.put(convert_from_python(k), convert_from_python(v))
                    return result
                  return value
                  
                # caller definition goes here (third argument)
                @polyglot.export_value
                %3$s  
                """;

        return pythonCodeTemplate.formatted(globalCode, functionAndExpression, pyCallerCode);
    }
}
