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
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.StateStore;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.axual.ksml.type.UserType.DEFAULT_NOTATION;

@Slf4j
public class PythonFunction extends UserFunction {
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper(true, new PythonDataObjectMapper(false, null));
    private static final Map<String, StateStore> EMPTY_STORES = new HashMap<>();
    private static final String QUOTE = "\"";
    private final DataObjectConverter converter;
    private final Value function;

    public static PythonFunction forFunction(PythonContext context, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, "function", name, definition);
    }

    public static PythonFunction forGenerator(PythonContext context, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, "generator", name, definition);
    }

    public static PythonFunction forPredicate(PythonContext context, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, "condition", name, definition);
    }

    private PythonFunction(PythonContext context, String namespace, String type, String name, FunctionDefinition definition) {
        super(namespace, name, definition.parameters(), definition.resultType(), definition.storeNames());
        converter = context.converter();
        final var pyCode = generatePythonCode(namespace, type, name, definition);
        function = context.registerFunction(pyCode, name + "_caller");
        if (function == null) {
            log.error("""
                    Function {} {}
                    Error in generated Python code:
                    
                    {}
                    """, namespace, name, pyCode);
            throw new ExecutionException("Error in function: %s.%s".formatted(namespace, name));
        }
    }

    @Override
    public DataObject call(StateStores stores, DataObject... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (this.fixedParameterCount > parameters.length) {
            throw new TopologyException("Function %s.%s - parameter list does not match function spec: minimally expected %d, got %d".formatted(namespace, name, this.parameters.length, parameters.length));
        }
        if (this.parameters.length < parameters.length) {
            throw new TopologyException("Function %s.%s - parameter list does not match function spec: maximally expected %d, got %d".formatted(namespace, name, this.parameters.length, parameters.length));
        }
        // Validate the parameter types
        for (int index = 0; index < parameters.length; index++) {
            final var declaredParameter = this.parameters[index];
            final var actualParameter = parameters[index];
            if (!declaredParameter.type().isAssignableFrom(actualParameter)) {
                throw new TopologyException("Function %s.%s expects parameter #%d (\"%s\") to be %s but %s was passed in ".formatted(namespace, name, index + 1, declaredParameter.name(), declaredParameter.type(), actualParameter.type()));
            }
        }

        // Check all parameters and copy them into the interpreter as prefixed globals
        var globalVars = new HashMap<String, Object>();
        globalVars.put("stores", stores != null ? stores : EMPTY_STORES);
        var arguments = convertParameters(globalVars, parameters);

        try {
            // Call the prepared function
            log.debug("Calling Python function \"{}\" \"{}\" with arguments {}", namespace, name, arguments);
            Value pyResult = function.execute(arguments);

            if (pyResult.canExecute()) {
                throw new ExecutionException("Python function %s.%s - Code results in a function instead of a value".formatted(namespace, name));
            }

            // Check if the function is supposed to return a result value
            if (resultType != null) {
                DataObject result = MAPPER.toDataObject(resultType.dataType(), pyResult);
                logCall(parameters, result);
                result = converter != null ? converter.convert(DEFAULT_NOTATION, result, resultType) : result;
                checkType(resultType.dataType(), result);
                return result;
            } else {
                logCall(parameters, null);
                return DataNull.INSTANCE;
            }
        } catch (Exception e) {
            logCall(parameters, null);
            throw FatalError.reportAndExit(new TopologyException("Error while executing function %s.%s : %s".formatted(namespace, name, e.getMessage()), e));
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

    private String generatePythonCode(String namespace, String type, String name, FunctionDefinition definition) {
        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(definition.code()).map(line -> "  " + line).toArray(String[]::new);
        String[] expressionCode = Arrays.stream(definition.expression()).map(line -> "    " + line).toArray(String[]::new);

        // Prepare a list of parameters for the function definition
        String[] defParams = Arrays.stream(definition.parameters()).map(p -> p.name() + (p.isOptional() ? "=None" : "")).toArray(String[]::new);
        // Prepare a list of parameters for the function calling
        String[] callParams = Arrays.stream(definition.parameters()).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        final var globalCode = String.join("\n", injectFunctionLocalVariables(namespace, type, definition.globalCode())) + "\n";

        // Code to include all global variables
        final var assignStores = definition.storeNames().stream()
                .map(storeName -> "  " + storeName + " = stores[\"" + storeName + "\"]\n")
                .collect(Collectors.joining());
        // Code to copy / initialize all global variables
        final var includeGlobals = """
                  global stores
                """;
        final var initializeGlobals = """
                  stores = convert_to_python(globalVars["stores"])
                """;
        // Code to initialize optional parameters with default values
        final var initializeOptionalParams = Arrays.stream(definition.parameters())
                .filter(ParameterDefinition::isOptional)
                .filter(p -> p.defaultValue() != null)
                .map(p -> "  if " + p.name() + " is None:\n    " + p.name() + " = " + (p.type() == DataString.DATATYPE ? QUOTE : "") + p.defaultValue() + (p.type() == DataString.DATATYPE ? QUOTE : "") + "\n")
                .collect(Collectors.joining());

        // Prepare the return statement
        final var returnStatement = "return" +
                (definition.resultType() != null && definition.resultType().dataType() != DataNull.DATATYPE
                        ? " " + String.join("\n", expressionCode) + "\n"
                        : "")
                + "\n";

        // Compose the function (if any) and the return statement together
        final var functionAndExpression = "def " + name + "(" + String.join(",", defParams) + "):\n" +
                includeGlobals +
                initFunctionLocalVariables(2, loggerName(namespace, type, name)) +
                assignStores +
                initializeOptionalParams +
                String.join("\n", functionCode) + "\n" +
                "  " + returnStatement + "\n";

        // Prepare the actual caller for the code
        final var convertedParams = Arrays.stream(callParams).map(p -> "convert_to_python(" + p + ")").toList();
        final var pyCallerCode = "def " + name + "_caller(globalVars," + String.join(",", defParams) + "):\n" +
                includeGlobals +
                initializeGlobals +
                "  return convert_from_python(" + name + "(" + String.join(",", convertedParams) + "))\n";

        final var pythonCodeTemplate =
                """
                        import polyglot
                        import java
                        
                        ArrayList = java.type('java.util.ArrayList')
                        HashMap = java.type('java.util.HashMap')
                        TreeMap = java.type('java.util.TreeMap')
                        stores = None
                        
                        # global Python code goes here (first argument)
                        %1$s
                        
                        # function definition and expression go here (second argument)
                        %2$s
                        
                        def convert_to_python(value):
                          if value == None: # don't modify to "is" operator, since Java's null is not exactly the same as None
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
                          if value == None: # don't modify to "is" operator, since Java's null is not exactly the same as None
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

    private String loggerName(String namespace, String type, String name) {
        return namespace + "." + type + "." + name;
    }

    private String[] injectFunctionLocalVariables(String namespace, String type, String[] code) {
        // Look for "def func():" statements and inject log variable code after all occurrences
        final var result = new ArrayList<String>();
        var injectCode = false;
        var defIndent = 0;
        var functionName = "";
        for (final var line : code) {
            if (line.trim().isEmpty()) continue;
            int lineIndent = line.length() - line.stripIndent().length();
            if (injectCode && lineIndent > defIndent) {
                result.add(initFunctionLocalVariables(lineIndent, loggerName(namespace, type, functionName)));
            }
            result.add(line);
            injectCode = false;
            if (line.trim().startsWith("def ") && line.trim().endsWith(":")) {
                final var functionDef = line.trim().substring(4, line.length() - 1).trim();
                if (functionDef.contains("(") && functionDef.endsWith(")")) {
                    injectCode = true;
                    defIndent = lineIndent;
                    functionName = functionDef.substring(0, functionDef.indexOf("("));
                }
            }
        }
        return result.toArray(String[]::new);
    }

    private String initFunctionLocalVariables(int indentCount, String loggerName) {
        final var indent = " ".repeat(indentCount);
        return indent + "global loggerBridge\n" +
                indent + "log = None\n" +
                indent + "if loggerBridge is not None:\n" +
                indent + "  log = loggerBridge.getLogger(\"" + loggerName + "\")\n" +
                indent + "global metrics\n";
    }
}
