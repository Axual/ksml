package io.axual.ksml.notation.avro;

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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;

public class AvroObject implements GenericRecord {
    private static final AvroSchemaMapper schemaMapper = new AvroSchemaMapper();
    private final StructSchema schema;
    private final Map<String, Object> data = new HashMap<>();
    private final GenericData validator = GenericData.get();
    private Schema avroSchema = null;

    public AvroObject(StructSchema schema, Map<?, ?> source) {
        if (schema == null) {
            throw FatalError.executionError("Can not create an AVRO object without schema");
        }
        this.schema = schema;
        schema.fields().forEach(field -> put(field.name(), source.get(field.name())));
    }

    @Override
    public void put(String key, Object value) {
        var field = schema.field(key);

        if (field.schema().type() == DataSchema.Type.STRUCT && value instanceof Map) {
            value = new AvroObject((StructSchema) field.schema(), (Map<?, ?>) value);
        }
        if (field.schema().type() == DataSchema.Type.ENUM) {
            value = new GenericData.EnumSymbol(schemaMapper.fromDataSchema(field.schema()), value != null ? value.toString() : null);
        }

        var fieldSchema = schemaMapper.fromDataSchema(field.schema());
        if (fieldSchema != null && !validator.validate(fieldSchema, value)) {
            throw KSMLDataException.validationFailed(key, value);
        }

        data.put(key, value);
    }

    @Override
    public Object get(String key) {
        return data.get(key);
    }

    @Override
    public void put(int index, Object value) {
        put(schema.fields().get(index).name(), value);
    }

    @Override
    public Object get(int index) {
        return data.get(schema.fields().get(index).name());
    }

    @Override
    public Schema getSchema() {
        if (avroSchema == null) {
            avroSchema = schemaMapper.fromDataSchema(schema);
        }
        return avroSchema;
    }
}
