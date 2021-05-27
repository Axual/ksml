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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.python.core.PyBoolean;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.parser.SchemaLoader;
import io.axual.ksml.type.Tuple;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.util.StringUtil;

public class PythonFunction extends UserFunction {
    private static final String TYPE_FIELD = "@type";
    protected final PythonInterpreter interpreter;

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
            interpreter.set(name + "_" + this.parameters[index].name, convertTo(parameters[index]));
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

                Object result = convertFrom(pyResult);
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

    public PyObject convertTo(Object object) {
        if (object == null) return null;
        if (object instanceof Boolean) {
            return new PyBoolean((Boolean) object);
        }
        if (object instanceof Float) {
            return new PyFloat((Float) object);
        }
        if (object instanceof Double) {
            return new PyFloat((Double) object);
        }
        if (object instanceof Integer) {
            return new PyInteger((Integer) object);
        }
        if (object instanceof Long) {
            return new PyLong((Long) object);
        }
        if (object instanceof Utf8) {
            return new PyString(((Utf8) object).toString());
        }
        if (object instanceof String) {
            return new PyString((String) object);
        }
        if (object instanceof GenericRecord) {
            return convertAvroToPython((GenericRecord) object);
        }
        if (object instanceof GenericData.EnumSymbol) {
            return new PyString(object.toString());
        }
        if (object instanceof Tuple) {
            var tuple = (Tuple) object;
            var elements = new PyObject[tuple.size()];
            for (var index = 0; index < tuple.size(); index++) {
                elements[index] = convertTo(tuple.get(index));
            }
            return new PyTuple(elements);
        }
        if (object instanceof List) {
            List<PyObject> convertedValues = new ArrayList<>();
            ((List<?>) object).forEach(v -> convertedValues.add(convertTo(v)));
            return new PyList(convertedValues);
        }
        if (object instanceof Map) {
            Map<PyObject, PyObject> convertedValues = new HashMap<>();
            ((Map<?, ?>) object).forEach((k, v) -> convertedValues.put(convertTo(k), convertTo(v)));
            return new PyDictionary(convertedValues);
        }
        if (object instanceof IndexedRecord) {
            final IndexedRecord record = (IndexedRecord) object;
            var schema = record.getSchema();
            if (schema.getType() == Schema.Type.RECORD) {
                Map<PyObject, PyObject> convertedValues = new HashMap<>();
                for (Schema.Field field : schema.getFields()) {
                    convertedValues.put(convertTo(field.name()), convertTo(record.get(field.pos())));
                }
                return new PyDictionary(convertedValues);
            }
        }
        throw new KSMLTopologyException("Can not convert type to Python: " + object.getClass().getSimpleName());
    }

    public Object convertFrom(PyObject object) {
        if (object == null) return null;
        if (object instanceof PyBoolean) {
            return object.asInt() != 0;
        }
        if (object instanceof PyFloat) {
            return object.asDouble();
        }
        if (object instanceof PyInteger) {
            return object.asInt();
        }
        if (object instanceof PyLong) {
            return object.asLong();
        }
        if (object instanceof PyString) {
            return ((PyString) object).toString();
        }
        if (object instanceof PyTuple) {
            PyTuple tuple = (PyTuple) object;
            var elements = new Object[tuple.size()];
            for (var index = 0; index < tuple.size(); index++) {
                elements[index] = convertFrom(tuple.pyget(index));
            }
            return new Tuple(elements);
        }
        if (object instanceof PyList) {
            List<Object> convertedList = new ArrayList<>();
            for (PyObject element : ((PyList) object).getArray()) {
                convertedList.add(convertFrom(element));
            }
            return convertedList;
        }
        if (object instanceof PyDictionary) {
            Map<String, Object> convertedMap = new HashMap<>();
            for (Map.Entry<PyObject, PyObject> entry : ((PyDictionary) object).getMap().entrySet()) {
                Object key = convertFrom(entry.getKey());
                if (key instanceof String) {
                    convertedMap.put((String) key, convertFrom(entry.getValue()));
                }
            }
            if (convertedMap.containsKey(TYPE_FIELD)) {
                String schemaName = (String) convertedMap.get(TYPE_FIELD);
                convertedMap.remove(TYPE_FIELD);
                return convertPythonToAvro(schemaName, convertedMap);
            }
            return convertedMap;
        }
        throw new KSMLTopologyException("Can not convert type from Python: " + object.getClass().getSimpleName());
    }

    private PyDictionary convertAvroToPython(GenericRecord record) {
        var result = new PyDictionary();
        for (Schema.Field field : record.getSchema().getFields()) {
            result.put(new PyString(field.name()), convertTo(record.get(field.name())));
        }
        result.put(new PyString(TYPE_FIELD), new PyString(record.getSchema().getFullName()));
        return result;
    }

    private GenericRecord convertPythonToAvro(String schemaName, Map<String, Object> record) {
        final var schema = SchemaLoader.load(schemaName);
        return new AvroObject(schema, record);
    }
}
