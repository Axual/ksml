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

import io.axual.ksml.data.mapper.PythonUserObjectMapper;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.util.StringUtil;

public class PythonFunction extends UserFunction {
    private static final String PYTHON = "python";
    protected static final Context context = Context.newBuilder(PYTHON)
            .allowNativeAccess(true)
            .allowPolyglotAccess(PolyglotAccess.ALL)
            .allowHostAccess(HostAccess.ALL)
            .allowHostClassLookup(name -> name.equals("java.util.ArrayList") || name.equals("java.util.HashMap"))
            .build();
    private static final PythonUserObjectMapper mapper = new PythonUserObjectMapper(context);
    protected final Value function;

    public PythonFunction(String name, FunctionDefinition definition) {
        super(name, definition.parameters, definition.resultType);

        // Prepend two spaces of indentation before the function code
        String[] functionCode = Arrays.stream(definition.code).map(line -> "  " + line).toArray(String[]::new);

        // Prepare a list of parameter names
        String[] params = Arrays.stream(parameters).map(p -> p.name).toArray(String[]::new);

        // Prepare the Python code to load
        String pyCode = "import polyglot\n" +
                "import java\n" +
                "ArrayList = java.type('java.util.ArrayList')\n" +
                "HashMap = java.type('java.util.HashMap')\n" +
                StringUtil.join("\n", definition.globalCode) + "\n" +
                "@polyglot.export_value\n" +
                "def " + name + "_function(" + StringUtil.join(",", params) + "):\n" +
                StringUtil.join("\n", functionCode) + "\n" +
                "  return" + (resultType != null ? " " + definition.expression : "") + "\n" +
                "\n" +
                "def convert_to(value):\n" +
                "  print('In convert_to: ')\n" +
                "  print(value)\n" +
                "  return value\n" +
                "\n" +
                "def convert_from(value):\n" +
//                "  print('In convert_from: ')\n" +
//                "  print(value)\n" +
                "  if isinstance(value, (list, tuple)):\n" +
//                "    print('Converting list')\n" +
                "    result = ArrayList()\n" +
                "    for e in value:\n" +
                "      result.add(convert_from(e))\n" +
                "    return result\n" +
                "  if type(value) is dict:\n" +
//                "    print('Converting dict')\n" +
                "    result = HashMap()\n" +
                "    for k, v in value.items():\n" +
                "      result.put(convert_from(k),convert_from(v))\n" +
                "    return result\n" +
                "  return value\n" +
                "\n" +
                "@polyglot.export_value\n" +
                "def " + name + "_caller(" + StringUtil.join(",", params) + "):\n" +
                "  return convert_from(" + name + "_function(" + StringUtil.join(",", params) + "))\n";

        Source script = Source.create(PYTHON, pyCode);
        context.eval(script);
        function = context.getPolyglotBindings().getMember(name + "_caller");
    }

    @Override
    public UserObject call(UserObject... parameters) {
        // Validate that the defined parameter list matches the amount of passed in parameters
        if (this.parameters.length != parameters.length) {
            throw new KSMLTopologyException("Parameter list does not match function spec: expected " + this.parameters.length + ", got " + parameters.length);
        }

        // Check all parameters and copy them into the interpreter as prefixed globals
        Object[] arguments = new Object[parameters.length];
        for (var index = 0; index < parameters.length; index++) {
            checkType(this.parameters[index], parameters[index]);
            arguments[index] = mapper.fromUserObject(parameters[index]);
        }

        try {
            // Call the prepared function
            Value pyResult = function.execute(arguments);

            if (pyResult.canExecute()) {
                throw new KSMLExecutionException("Python code results in a function instead of a value");
            }

            // Check if the function is supposed to return a result value
            if (resultType != null) {
                // The converted result value from Python
                final UserObject result;

                // If a value is expected, but none is returned, then handle special cases
                if (pyResult.isNull()) {
                    // An empty string in YAML (ie. '') is returned as null by the parser, so
                    // when we expect a string and get null, we convert it to the empty string.
                    if (resultType.type() == UserString.DATATYPE) {
                        // Empty string may be returned as null, so catch and convert here
                        result = new UserString(resultType.notation(), "");
                    } else {
                        throw new KSMLTopologyException("Illegal return from function: null");
                    }
                } else {
                    // Convert the result object to a UserObject
                    result = mapper.toUserObject(resultType, pyResult);
                }

                logCall(parameters, result);
                checkType(resultType.type(), result);
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
