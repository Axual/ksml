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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.exception.KSMLTypeException;

// Json data is internally represented as List<Object> or Map<String,Object>. This converter maps
// any internal data structure to Json.
public class JsonTypeConverter implements TypeConverter<Object> {
    @Override
    public Object convert(Object object) {
        if (object == null) return null;

        // Only Lists, Maps and GenericRecords (AVRO) are convertible to Json
        if (object instanceof List || object instanceof Map || object instanceof GenericRecord) {
            return convertInternal(object);
        }

        throw KSMLTypeException.conversionFailed(object.getClass(), Object.class);
    }

    private Object convertInternal(Object object) {
        if (object == null) return null;

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
        if (object instanceof GenericRecord) return convertFrom((GenericRecord) object);
        if (object instanceof GenericData.EnumSymbol)
            return convertFrom((GenericData.EnumSymbol) object);

        throw KSMLTypeException.conversionFailed(object.getClass(), Object.class);
    }

    private String convertFrom(Utf8 object) {
        return object.toString();
    }

    private List<Object> convertFrom(List<?> object) {
        List<Object> result = new ArrayList<>();
        object.forEach(v -> result.add(convertInternal(v)));
        return result;
    }

    private Map<String, Object> convertFrom(Map<?, ?> object) {
        var result = new HashMap<String, Object>();
        object.forEach((k, v) -> result.put(convertInternal(k).toString(), convertInternal(v)));
        return result;
    }

    private Map<String, Object> convertFrom(GenericRecord object) {
        var schema = object.getSchema();
        if (schema.getType() == Schema.Type.RECORD) {
            var result = new HashMap<String, Object>();
            for (Schema.Field field : object.getSchema().getFields()) {
                result.put(field.name(), convertInternal(object.get(field.name())));
            }
            return result;
        }
        throw KSMLTypeException.conversionFailed(GenericRecord.class, Map.class);
    }

    private Object convertFrom(GenericData.EnumSymbol object) {
        return object.toString();
    }
}
