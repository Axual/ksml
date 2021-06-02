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
import io.axual.ksml.parser.SchemaLoader;

import static io.axual.ksml.data.AvroObject.AVRO_TYPE_FIELD;

public class AvroTypeConverter implements TypeConverter<GenericRecord> {
    @Override
    public GenericRecord convert(Object object) {
        if (object == null) return null;

        // Only Maps and GenericRecords (AVRO) are mappable to Json
        if (object instanceof Map) return convertFrom((Map<?, ?>) object);

        // Convert from AVRO types
        if (object instanceof GenericRecord) return (GenericRecord) object;

        throw new KSMLTypeException(object.getClass(), Object.class);
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
        if (object instanceof GenericRecord) return object;
        if (object instanceof GenericData.EnumSymbol) return object;

        throw new KSMLTypeException(object.getClass(), Object.class);
    }

    private String convertFrom(Utf8 object) {
        return object.toString();
    }

    private Object convertFrom(List<?> object) {
        return new GenericData.Array<>(Schema.create(Schema.Type.ARRAY), new ArrayList<>(object));
    }

    private GenericRecord convertFrom(Map<?, ?> object) {
        Map<String, Object> convertedMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : object.entrySet()) {
            Object key = convertInternal(entry.getKey());
            if (key instanceof String) {
                convertedMap.put((String) key, convertInternal(entry.getValue()));
            }
        }

        if (convertedMap.containsKey(AVRO_TYPE_FIELD)) {
            String schemaName = (String) convertedMap.get(AVRO_TYPE_FIELD);
            convertedMap.remove(AVRO_TYPE_FIELD);
            final var schema = SchemaLoader.load(schemaName);
            return new AvroObject(schema, convertedMap);
        }

        throw new KSMLTypeException(object.getClass(), GenericRecord.class);
    }
}
