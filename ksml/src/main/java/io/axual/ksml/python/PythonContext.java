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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.util.Arrays;
import java.util.stream.Collectors;

public class PythonContext {
    private static final String PYTHON = "python";
    private static final String QUOTE = "\"";
    private final Context context = Context.newBuilder(PYTHON)
            .allowNativeAccess(true)
            .allowPolyglotAccess(PolyglotAccess.ALL)
            .allowHostAccess(HostAccess.ALL)
            .allowHostClassLookup(name -> name.equals("java.util.ArrayList") || name.equals("java.util.HashMap") || name.equals("java.util.TreeMap"))
            .build();
    private final DataObjectConverter converter;

    public PythonContext(DataObjectConverter converter) {
        this.converter = converter;
    }

    public DataObjectConverter getConverter() {
        return converter;
    }

    public Value registerFunction(String name, String[] globalVariables, FunctionDefinition definition) {
        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(definition.code).map(line -> "  " + line).toArray(String[]::new);

        // Prepare a list of parameters for the function definition
        String[] defParams = Arrays.stream(definition.parameters).map(p -> p.name() + (p.isOptional() ? "=None" : "")).toArray(String[]::new);
        // Prepare a list of parameters for the function calling
        String[] callParams = Arrays.stream(definition.parameters).map(ParameterDefinition::name).toArray(String[]::new);

        // prepare globalCode from the function definition
        final var globalCode = String.join("\n", definition.globalCode) + "\n";

        // Code to include all global variables
        final var includeGlobals = Arrays.stream(globalVariables)
                .map(g -> "  global " + g + "\n")
                .collect(Collectors.joining());
        // Code to copy / initialize all global variables
        final var initializeGlobals = Arrays.stream(globalVariables)
                .map(g -> "  " + g + " = convert_to_python(globalVars[\"" + g + "\"])\n")
                .collect(Collectors.joining());
        // Code to initialize optional parameters with default values
        final var initializeOptionalParams = Arrays.stream(definition.parameters)
                .filter(ParameterDefinition::isOptional)
                .filter(p -> p.defaultValue() != null)
                .map(p -> "  if " + p.name() + " == None:\n    " + p.name() + " = " + (p.type() == DataString.DATATYPE ? QUOTE : "") + p.defaultValue() + (p.type() == DataString.DATATYPE ? QUOTE : "") + "\n")
                .collect(Collectors.joining());

        // Prepare function (if any) and expression from the function definition
        final var functionAndExpression = "def " + name + "(" + String.join(",", defParams) + "):\n" +
                includeGlobals +
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

        final var pyCode = pythonCodeTemplate.formatted(globalCode, functionAndExpression, pyCallerCode);
        Source script = Source.create(PYTHON, pyCode);
        try {
            context.eval(script);
        } catch (Exception e) {
            System.out.println(e);
        }
        return context.getPolyglotBindings().getMember(name + "_caller");
    }
}
