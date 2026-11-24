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
import io.axual.ksml.definition.InjectedParameter;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.proxy.store.StoreIteratorProxy;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.axual.ksml.type.UserType.DEFAULT_NOTATION;

@Slf4j
public class PythonFunction extends UserFunction {
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    private static final String QUOTE = "\"";
    private final DataObjectConverter converter;
    private final Context context;
    private final PythonDataObjectMapper dataObjectMapper;
    private final Value function;

    public static PythonFunction forFunction(PythonContext context, FunctionDefinition definition) {
        return new PythonFunction(context, definition);
    }

    public static PythonFunction forGenerator(PythonContext context, FunctionDefinition definition) {
        return new PythonFunction(context, definition.withType(KSMLDSL.Functions.TYPE_GENERATOR));
    }

    public static PythonFunction forPredicate(PythonContext context, FunctionDefinition definition) {
        return new PythonFunction(context, definition.withType(KSMLDSL.Functions.TYPE_PREDICATE));
    }

    private PythonFunction(PythonContext context, FunctionDefinition def) {
        super(def);
        validateNoNameClashes(definition);
        converter = context.converter();
        this.context = context.context();
        dataObjectMapper = new PythonDataObjectMapper(true, this.context);
        final var pyCode = generatePythonCode(definition);
        function = context.registerFunction(pyCode, definition.name() + "_caller");
        if (function == null) {
            final var pyCodeLines = pyCode.split("\n");
            final var builder = new StringBuilder();
            for (int index = 1; index <= pyCodeLines.length; index++) {
                builder.append(index).append("  ").append(pyCodeLines[index - 1]).append("\n");
            }
            log.error("""
                    Function {} {}
                    Error in generated Python code:
                    
                    {}
                    """, definition.namespace(), definition.name(), builder);
            throw new ExecutionException("Error in function: %s.%s".formatted(definition.namespace(), definition.name()));
        }
    }

    // Parameters, injected parameters and stores all become local variables of the generated Python function. A name
    // used twice makes one variable silently replace the other.
    private static void validateNoNameClashes(FunctionDefinition definition) {
        final var namesInUse = new HashMap<String, String>();
        for (final var param : definition.parameters())
            checkNameNotInUse(definition, namesInUse, param.name(), "parameter");
        for (final var injected : definition.injectedParameters())
            checkNameNotInUse(definition, namesInUse, injected.name(), "injected variable");
        for (final var storeName : new LinkedHashSet<>(definition.storeNames()))
            checkNameNotInUse(definition, namesInUse, storeName, "store");
    }

    private static void checkNameNotInUse(FunctionDefinition definition, Map<String, String> namesInUse, String nameToAdd, String kind) {
        final var existing = namesInUse.putIfAbsent(nameToAdd, kind);
        if (existing != null)
            throw new TopologyException("The %s \"%s\" of function %s.%s clashes with the %s of the same name, rename the %s".formatted(kind, nameToAdd, definition.namespace(), definition.name(), existing, kind));
    }

    @Override
    public DataObject call(Map<String, Object> injections, DataObject... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (definition.fixedParameterCount() > parameters.length) {
            throw new TopologyException("Function %s.%s - parameter list does not match function spec: minimally expected %d, got %d".formatted(definition.namespace(), definition.name(), definition.parameters().length, parameters.length));
        }
        if (definition.parameters().length < parameters.length) {
            throw new TopologyException("Function %s.%s - parameter list does not match function spec: maximally expected %d, got %d".formatted(definition.namespace(), definition.name(), definition.parameters().length, parameters.length));
        }
        // Validate the parameter types
        for (int index = 0; index < parameters.length; index++) {
            final var declaredParameter = definition.parameters()[index];
            final var actualParameter = parameters[index];
            final var assignable = declaredParameter.type().isAssignableFrom(actualParameter);
            if (assignable.isNotAssignable()) {
                throw new TopologyException("Function %s.%s expects parameter #%d (\"%s\") to be %s but %s was passed in: %s".formatted(definition.namespace(), definition.name(), index + 1, declaredParameter.name(), declaredParameter.type(), actualParameter.type(), assignable));
            }
        }

        // Only the variables that the caller injects are set in Python
        for (final var variable : injections.keySet()) {
            if (!hasInjectedParameter(variable))
                throw new ExecutionException("Python function %s.%s - can not inject unknown variable '%s'".formatted(definition.namespace(), definition.name(), variable));
        }
        var arguments = convertParameters(injections, parameters);

        final int iteratorMark = StoreIteratorProxy.beginCall();
        try {
            // Call the prepared function
            log.debug("Calling Python function: namespace={}, name={}, arguments={}", definition.namespace(), definition.name(), arguments);
            Value pyResult = function.execute(arguments);

            if (pyResult.canExecute()) {
                throw new ExecutionException("Python function %s.%s - Code results in a function instead of a value".formatted(definition.namespace(), definition.name()));
            }

            // Check if the function is supposed to return a result value
            if (definition.resultType() != null) {
                DataObject result = dataObjectMapper.toDataObject(definition.resultType().dataType(), pyResult);
                logCall(parameters, result);
                // Only convert a value that does not fit yet; converting again copies every list, map and struct
                if (converter != null && definition.resultType().dataType().isAssignableFrom(result).isNotAssignable())
                    result = converter.convert(DEFAULT_NOTATION, result, definition.resultType());
                checkType(definition.resultType().dataType(), result);
                return result;
            } else {
                logCall(parameters, null);
                return DataNull.INSTANCE;
            }
        } catch (Exception e) {
            logCall(parameters, null);
            throw FatalError.report(new TopologyException("Error while executing function %s.%s : %s".formatted(definition.namespace(), definition.name(), e.getMessage()), e));
        } finally {
            // Closes store iterators that the function left open, for example after break or return in a loop
            final int closed = StoreIteratorProxy.endCall(iteratorMark);
            if (closed > 0) log.debug("Closed {} store iterator(s) left open by {}.{}", closed, definition.namespace(), definition.name());
        }
    }

    private Object[] convertParameters(Map<String, Object> injections, DataObject... parameters) {
        Object[] result = new Object[parameters.length + 1];
        result[0] = NATIVE_MAPPER.toRealPythonValue(context, injections);
        for (var index = 0; index < parameters.length; index++) {
            checkType(definition.parameters()[index], parameters[index]);
            // Convert DataObject to Python value
            result[index + 1] = dataObjectMapper.fromDataObject(parameters[index]);
        }
        return result;
    }

    private static String generatePythonCode(FunctionDefinition definition) {
        // Prepend two spaces of indentation before the function code
        String[] functionCode = getFunctionCode(definition.code(), "  ");
        String[] expressionCode = Arrays.stream(definition.expression()).map(line -> "    " + line).toArray(String[]::new);

        // Prepare a list of parameters for the function definition
        String[] defParams = Arrays.stream(definition.parameters()).map(p -> p.name() + (p.isOptional() ? "=None" : "")).toArray(String[]::new);
        // Prepare a list of parameters for the function calling
        String[] callParams = Arrays.stream(definition.parameters()).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        String[] globalCodeLines = getFunctionCode(definition.globalCode(), "");
        final var globalCode = String.join("\n", globalCodeLines) + "\n";

        // The classes of the ksml module that wrap injected objects
        final var proxyCode = definition.injectedParameters().stream()
                .map(InjectedParameter::proxyClassName)
                .filter(Objects::nonNull)
                .distinct()
                .map(className -> "from " + PythonContext.KSML_MODULE_NAME + " import " + className)
                .collect(Collectors.joining("\n"));

        // Prepare the return statement
        final var returnStatement = "return" +
                (definition.resultType() != null && definition.resultType().dataType() != DataNull.DATATYPE
                        ? " " + String.join("\n", expressionCode) + "\n"
                        : "")
                + "\n";

        // Compose the function (if any) and the return statement together
        final var functionParams = String.join(",", defParams) + (defParams.length > 0 ? ", " : "") + "*, _ksml_injected=None";
        final var functionAndExpression = "def " + definition.name() + "(" + functionParams+ "):\n" +
                // Code to read the injected parameters, including the stores, as local variables of the function
                initializeInjectedParams(definition.injectedParameters()) +
                // Code to assign store names as variables
                initializeStoreNames(definition.storeNames()) +
                // Code to initialize optional parameters with default values
                initializeOptionalParams(definition.parameters()) +
                String.join("\n", functionCode) + "\n" +
                "  " + returnStatement + "\n";

        // Prepare the actual caller for the code
        // Parameters are now pre-converted to Python types by PythonTypeConverter in Java
        // Return value is passed as-is; PythonDataObjectMapper handles GraalVM Value conversion
        // Sets the function that the log variable logs for, also in helper functions, and restores it afterwards because
        // calls can nest (a punctuator's context.forward runs a filter). Its own names start with _ksml_, so they do not
        // hide a function or parameter.
        final var callerParams = String.join(",", callParams) + (callParams.length > 0 ? ", " : "") + "_ksml_injected=_ksml_injected";
        final var pyCallerCode = "def " + definition.name() + "_caller(_ksml_injected," + String.join(",", defParams) + "):\n" +
                "  _ksml_token = _ksml_current_function.set(\"" + loggerName(definition.namespace(), definition.type(), definition.name()) + "\")\n" +
                "  try:\n" +
                "    return " + definition.name() + "(" + callerParams + ")\n" +
                "  finally:\n" +
                "    _ksml_current_function.reset(_ksml_token)\n";

        // Python code template - no longer needs convert_to_python/convert_from_python
        // as Java-side PythonTypeConverter handles all collection conversion
        final var pythonCodeTemplate =
                """
                        import polyglot
                        from ksml import _current_function as _ksml_current_function
                        _ksml_injected_parameters = {}

                        # imports of the Python classes for injected parameters go here (fourth argument)
                        %4$s
                        
                        # global Python code goes here (first argument)
                        %1$s
                        
                        # function definition and expression go here (second argument)
                        %2$s
                        
                        # caller definition goes here (third argument)
                        @polyglot.export_value
                        %3$s
                        """;

        return pythonCodeTemplate.formatted(globalCode, functionAndExpression, pyCallerCode, proxyCode);
    }

    private static String[] getFunctionCode(String[] code, String spaces) {
        return Arrays.stream(code).map(line -> spaces + line).toArray(String[]::new);
    }

    private static String initializeInjectedParams(List<InjectedParameter> params) {
        final var result = new StringBuilder();
        for (final var param : params) {
            final var value = "(_ksml_injected or {}).get(\"" + param.name() + "\")";
            result.append("  ").append(param.name()).append(" = ");
            if (param.proxyClassName() != null)
                result.append(param.proxyClassName()).append("(").append(value).append(") if ").append(value).append(" is not None else None\n");
            else
                result.append(value).append("\n");
        }
        return result.toString();
    }

    private static String initializeStoreNames(List<String> storeNames) {
        return storeNames.stream()
                .map(storeName -> "  " + storeName + " = stores[\"" + storeName + "\"]\n")
                .collect(Collectors.joining());
    }

    private static String initializeOptionalParams(ParameterDefinition[] parameters) {
        return Arrays.stream(parameters)
                .filter(ParameterDefinition::isOptional)
                .filter(p -> p.defaultValue() != null)
                .map(p -> "  if " + p.name() + " is None:\n    " + p.name() + " = " + (p.type() == DataString.DATATYPE ? QUOTE : "") + p.defaultValue() + (p.type() == DataString.DATATYPE ? QUOTE : "") + "\n")
                .collect(Collectors.joining());
    }
    private static String loggerName(String namespace, String type, String name) {
        return namespace + "." + type + "." + name;
    }
}
