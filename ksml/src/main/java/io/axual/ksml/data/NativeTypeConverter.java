package io.axual.ksml.data;

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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.exception.KSMLTypeException;
import io.axual.ksml.parser.SchemaLoader;
import io.axual.ksml.type.Tuple;

import static io.axual.ksml.data.AvroObject.AVRO_TYPE_FIELD;

// Converter to transform any representation data object into a Native representation. Native
// types are primitive types (Boolean, Integer etc), Lists and Maps (typically Json data) and
// GenericRecords (AVRO).
public class NativeTypeConverter implements TypeConverter<Object> {
    @Override
    public Object convert(Object object) {
        if (object == null) return null;

        // Convert from Python types
        if (object instanceof PyObject) return convertFrom((PyObject) object);

        // Convert from primitive types
        if (object instanceof Boolean) return object;
        if (object instanceof Float) return object;
        if (object instanceof Double) return object;
        if (object instanceof Integer) return object;
        if (object instanceof Long) return object;
        if (object instanceof Utf8) return convertFrom((Utf8) object);
        if (object instanceof String) return object;

        // Convert from Json types
        if (object instanceof List) return convertFrom((List<?>) object);
        if (object instanceof Map) return convertFrom((Map<?, ?>) object);

        // Convert from AVRO types
        if (object instanceof GenericRecord) return object;
        if (object instanceof GenericData.EnumSymbol) return object;

        throw KSMLTypeException.conversionFailed(object.getClass(), Object.class);
    }

    private Object convertFrom(PyObject object) {
        if (object instanceof PyBoolean) return convertFromPython((PyBoolean) object);
        if (object instanceof PyFloat) return convertFromPython((PyFloat) object);
        if (object instanceof PyInteger) return convertFromPython((PyInteger) object);
        if (object instanceof PyLong) return convertFromPython((PyLong) object);
        if (object instanceof PyString) return convertFromPython((PyString) object);
        if (object instanceof PyTuple) return convertFromPython((PyTuple) object);
        if (object instanceof PyList) return convertFromPython((PyList) object);
        if (object instanceof PyDictionary) return convertFromPython((PyDictionary) object);

        throw KSMLTypeException.conversionFailed(object.getClass(), Object.class);
    }

    private boolean convertFromPython(PyBoolean object) {
        return object.asInt() != 0;
    }

    private double convertFromPython(PyFloat object) {
        return object.asDouble();
    }

    private int convertFromPython(PyInteger object) {
        return object.asInt();
    }

    private long convertFromPython(PyLong object) {
        return object.asLong();
    }

    private String convertFromPython(PyString object) {
        return object.toString();
    }

    private Tuple convertFromPython(PyTuple object) {
        var elements = new Object[object.size()];
        for (var index = 0; index < object.size(); index++) {
            elements[index] = convertFrom(object.pyget(index));
        }
        return new Tuple(elements);
    }

    private List<Object> convertFromPython(PyList object) {
        List<Object> convertedList = new ArrayList<>();
        for (PyObject element : object.getArray()) {
            convertedList.add(convertFrom(element));
        }
        return convertedList;
    }

    private Object convertFromPython(PyDictionary object) {
        Map<String, Object> convertedMap = new HashMap<>();
        for (Map.Entry<PyObject, PyObject> entry : object.getMap().entrySet()) {
            var key = convertFrom(entry.getKey());
            if (key instanceof String) {
                convertedMap.put((String) key, convertFrom(entry.getValue()));
            }
        }

        return convertFrom(convertedMap);
    }

    private String convertFrom(Utf8 object) {
        return object.toString();
    }

    private List<Object> convertFrom(List<?> object) {
        List<Object> result = new ArrayList<>();
        object.forEach(v -> result.add(convert(v)));
        return result;
    }

    // Convert a given Map to a normalized/standardized Map or a GenericRecord (AVRO)
    private Object convertFrom(Map<?, ?> object) {
        Map<String, Object> result = new HashMap<>();
        object.forEach((k, v) -> result.put(convert(k).toString(), convert(v)));
        if (result.containsKey(AVRO_TYPE_FIELD)) {
            String schemaName = (String) result.get(AVRO_TYPE_FIELD);
            result.remove(AVRO_TYPE_FIELD);
            final var schema = SchemaLoader.load(schemaName);
            return new AvroObject(schema, result);
        }
        return result;
    }
}
