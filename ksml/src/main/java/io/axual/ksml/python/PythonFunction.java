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


import com.codahale.metrics.Timer;

import io.axual.ksml.data.tag.ContextTags;
import org.apache.kafka.streams.processor.StateStore;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.metric.MetricName;
import io.axual.ksml.metric.KSMLMetrics;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;

import static io.axual.ksml.data.notation.UserType.DEFAULT_NOTATION;

@Slf4j
public class PythonFunction extends UserFunction {
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper(true);
    private static final Map<String, StateStore> EMPTY_STORES = new HashMap<>();
    private static final String QUOTE = "\"";
    private final DataObjectConverter converter;
    private final Value function;
    private final Timer functionTimer;

    public static PythonFunction forFunction(PythonContext context, ContextTags tags, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, tags, "function", name, definition);
    }

    public static PythonFunction forGenerator(PythonContext context, ContextTags tags, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, tags, "generator", name, definition);
    }

    public static PythonFunction forPredicate(PythonContext context, ContextTags tags, String namespace, String name, FunctionDefinition definition) {
        return new PythonFunction(context, namespace, tags, "condition", name, definition);
    }

    private PythonFunction(PythonContext context, String namespace, ContextTags tags, String type, String name, FunctionDefinition definition) {
        super(name, definition.parameters(), definition.resultType(), definition.storeNames());
        converter = context.converter();
        final var pyCode = generatePythonCode(namespace, type, name, definition);
        function = context.registerFunction(pyCode, name + "_caller");
        if (function == null) {
            System.out.println("Error in generated Python code:\n" + pyCode);
            throw new ExecutionException("Error in function: " + name);
        }
        var metricName = new MetricName("execution-time", tags.append("function-type", type).append("function-name", name));
        if (KSMLMetrics.registry().getTimer(metricName) == null) {
            functionTimer = KSMLMetrics.registry().registerTimer(metricName);
        } else {
            functionTimer = KSMLMetrics.registry().getTimer(metricName);
        }
    }

    @Override
    public DataObject call(StateStores stores, DataObject... parameters) {
        return functionTimer.timeSupplier(() -> {
            // Validate that the defined parameter list matches the amount of passed in parameters
            if (this.fixedParameterCount > parameters.length) {
                throw new TopologyException("Parameter list does not match function spec: minimally expected " + this.parameters.length + ", got " + parameters.length);
            }
            if (this.parameters.length < parameters.length) {
                throw new TopologyException("Parameter list does not match function spec: maximally expected " + this.parameters.length + ", got " + parameters.length);
            }
            // Validate the parameter types
            for (int index = 0; index < parameters.length; index++) {
                if (!this.parameters[index].type().isAssignableFrom(parameters[index])) {
                    throw new TopologyException("User function \"" + name + "\" expects parameter " + (index + 1) + " (\"" + this.parameters[index].name() + "\") to be " + this.parameters[index].type() + ", but " + parameters[index].type() + " was passed in");
                }
            }

            // Check all parameters and copy them into the interpreter as prefixed globals
            var globalVars = new HashMap<String, Object>();
            globalVars.put("stores", stores != null ? stores : EMPTY_STORES);
            var arguments = convertParameters(globalVars, parameters);

            try {
                // Call the prepared function
                log.debug("Calling Python function \"{}\" with arguments {}", name, arguments);
                Value pyResult = function.execute(arguments);

                if (pyResult.canExecute()) {
                    throw new ExecutionException("Python code results in a function instead of a value");
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
                throw FatalError.reportAndExit(new TopologyException("Error while executing function " + name + ": " + e.getMessage(), e));
            }
        });
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

        // Prepare a list of parameters for the function definition
        String[] defParams = Arrays.stream(definition.parameters()).map(p -> p.name() + (p.isOptional() ? "=None" : "")).toArray(String[]::new);
        // Prepare a list of parameters for the function calling
        String[] callParams = Arrays.stream(definition.parameters()).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        final var globalCode = String.join("\n", injectLogVariable(namespace, type, definition.globalCode())) + "\n";

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

        // Prepare function (if any) and expression from the function definition
        final var functionAndExpression = "def " + name + "(" + String.join(",", defParams) + "):\n" +
                includeGlobals +
                initLogCode(2, loggerName(namespace, type, name)) +
                assignStores +
                initializeOptionalParams +
                String.join("\n", functionCode) + "\n" +
                "  return" + (definition.resultType() != null && definition.resultType().dataType() != DataNull.DATATYPE ? " " + definition.expression() : "") + "\n" +
                "\n";

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
                          if value == None: # don't modify to "is" operator, since that Java's null is not exactly the same as None
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
                          if value == None: # don't modify to "is" operator, since that Java's null is not exactly the same as None
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

    private String[] injectLogVariable(String namespace, String type, String[] code) {
        // Look for "def func():" statements and inject log variable code after all occurrences
        final var result = new ArrayList<String>();
        var injectCode = false;
        var defIndent = 0;
        var functionName = "";
        for (final var line : code) {
            if (line.trim().isEmpty()) continue;
            int lineIndent = line.length() - line.stripIndent().length();
            if (injectCode && lineIndent > defIndent) {
                result.add(initLogCode(lineIndent, loggerName(namespace, type, functionName)));
            }
            result.add(line);
            injectCode = false;
            if (line.trim().startsWith("def ") && line.trim().endsWith(":")) {
                final var function = line.trim().substring(4, line.length() - 1).trim();
                if (function.contains("(") && function.endsWith(")")) {
                    injectCode = true;
                    defIndent = lineIndent;
                    functionName = function.substring(0, function.indexOf("("));
                }
            }
        }
        return result.toArray(String[]::new);
    }

    private String initLogCode(int indentCount, String loggerName) {
        final var indent = " ".repeat(indentCount);
        return indent + "global loggerBridge\n" +
                indent + "log = None\n" +
                indent + "if loggerBridge is not None:\n" +
                indent + "  log = loggerBridge.getLogger(\"" + loggerName + "\")\n";
    }
}
