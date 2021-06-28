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

import org.apache.avro.Schema;
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
import io.axual.ksml.type.Tuple;

import static io.axual.ksml.data.AvroObject.AVRO_TYPE_FIELD;

public class PythonTypeConverter implements TypeConverter<PyObject> {
    @Override
    public PyObject convert(Object object) {
        if (object == null) return null;

        // Convert from primitive types
        if (object instanceof Boolean) return convertToPython((Boolean) object);
        if (object instanceof Float) return convertToPython((Float) object);
        if (object instanceof Double) return convertToPython((Double) object);
        if (object instanceof Integer) return convertToPython((Integer) object);
        if (object instanceof Long) return convertToPython((Long) object);
        if (object instanceof Utf8) return convertToPython((Utf8) object);
        if (object instanceof String) return convertToPython((String) object);
        if (object instanceof Tuple) return convertToPython((Tuple) object);

        // Convert from Json types
        if (object instanceof List) return convertToPython((List<?>) object);
        if (object instanceof Map) return convertToPython((Map<?, ?>) object);

        // Convert from AVRO types
        if (object instanceof GenericRecord) return convertToPython((GenericRecord) object);
        if (object instanceof GenericData.EnumSymbol)
            return convertToPython((GenericData.EnumSymbol) object);

        throw KSMLTypeException.conversionFailed(object.getClass(), PyObject.class);
    }

    private PyBoolean convertToPython(Boolean object) {
        return new PyBoolean(object);
    }

    private PyFloat convertToPython(Float object) {
        return new PyFloat(object);
    }

    private PyFloat convertToPython(Double object) {
        return new PyFloat(object);
    }

    private PyInteger convertToPython(Integer object) {
        return new PyInteger(object);
    }

    private PyLong convertToPython(Long object) {
        return new PyLong(object);
    }

    private PyString convertToPython(Utf8 object) {
        return new PyString(object.toString());
    }

    private PyString convertToPython(String object) {
        return new PyString(object);
    }

    private PyTuple convertToPython(Tuple object) {
        var elements = new PyObject[object.size()];
        for (var index = 0; index < object.size(); index++) {
            elements[index] = convert(object.get(index));
        }
        return new PyTuple(elements);
    }

    private PyList convertToPython(List<?> object) {
        List<PyObject> convertedValues = new ArrayList<>();
        object.forEach(v -> convertedValues.add(convert(v)));
        return new PyList(convertedValues);
    }

    private PyDictionary convertToPython(Map<?, ?> object) {
        Map<PyObject, PyObject> convertedValues = new HashMap<>();
        object.forEach((k, v) -> convertedValues.put(convert(k), convert(v)));
        return new PyDictionary(convertedValues);
    }

    private PyDictionary convertToPython(GenericRecord object) {
        var schema = object.getSchema();
        if (schema.getType() == Schema.Type.RECORD) {
            var result = new PyDictionary();
            for (Schema.Field field : object.getSchema().getFields()) {
                result.put(new PyString(field.name()), convert(object.get(field.name())));
            }
            result.put(new PyString(AVRO_TYPE_FIELD), new PyString(object.getSchema().getFullName()));
            return result;
        }
        throw KSMLTypeException.conversionFailed(GenericRecord.class, PyDictionary.class);
    }

    private PyString convertToPython(GenericData.EnumSymbol object) {
        return new PyString(object.toString());
    }
}
