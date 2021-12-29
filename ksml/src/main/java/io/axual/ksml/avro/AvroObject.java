package io.axual.ksml.avro;

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

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.exception.KSMLTypeException;

public class AvroObject implements GenericRecord {
    private final Schema schema;
    private final Map<String, Object> data = new HashMap<>();
    private final GenericData validator = GenericData.get();

    public AvroObject(Schema schema, Map<?, ?> source) {
        this.schema = schema;
        schema.getFields().forEach(field -> put(field.name(), source.get(field.name())));
    }

    @Override
    public void put(String key, Object value) {
        var field = schema.getField(key);

        if (field.schema().getType() == Schema.Type.RECORD && value instanceof Map) {
            value = new AvroObject(field.schema(), (Map<?, ?>) value);
        }
        if (field.schema().getType() == Schema.Type.ENUM) {
            value = new GenericData.EnumSymbol(field.schema(), value != null ? value.toString() : null);
        }

        if (!validator.validate(field.schema(), value)) {
            throw KSMLTypeException.validationFailed(key, value);
        }

        data.put(key, value);
    }

    @Override
    public Object get(String key) {
        return data.get(key);
    }

    @Override
    public void put(int index, Object value) {
        put(schema.getFields().get(index).name(), value);
    }

    @Override
    public Object get(int index) {
        return data.get(schema.getFields().get(index).name());
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
