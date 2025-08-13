package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.StructSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Lightweight Avro GenericRecord implementation backed by a KSML StructSchema.
 *
 * <p>Used as a bridge object when converting DataStruct values to native objects
 * for Avro serdes. It validates field assignments against the derived Avro Schema
 * and supports nested structs and enums.</p>
 */
public class AvroObject implements GenericRecord {
    private static final AvroSchemaMapper AVRO_SCHEMA_MAPPER = new AvroSchemaMapper();
    private static final GenericData VALIDATOR = GenericData.get();
    private final StructSchema schema;
    private final Map<String, Object> data = new HashMap<>();
    private Schema avroSchema = null;

    /**
     * Construct an AvroObject backed by the provided StructSchema and source map.
     *
     * <p>Copies all fields from the source if present, otherwise uses field default values from the schema.</p>
     *
     * @param schema the struct schema that defines fields and types (required)
     * @param source a map of field values (may be null)
     * @throws io.axual.ksml.data.exception.DataException when schema is null
     */
    public AvroObject(StructSchema schema, Map<?, ?> source) {
        if (schema == null) throw new DataException("Can not create an AVRO object without schema");
        this.schema = schema;

        // Copy all fields from the source object, or fill in defaults when absent
        if (source != null) {
            schema.fields().forEach(field -> {
                final var fieldName = field.name();
                final var defaultValue = field.defaultValue() != null ? field.defaultValue().value() : null;
                final var fieldValue = source.containsKey(fieldName) ? source.get(fieldName) : defaultValue;
                put(fieldName, fieldValue);
            });
        }
    }

    /**
     * Put a field value by name, validating against the field schema.
     *
     * <p>When the field schema is a nested struct and value is a Map, a nested AvroObject is constructed.
     * Enum fields are represented as GenericData.EnumSymbol.</p>
     *
     * @param key   field name
     * @param value value to assign (may be null when field is optional)
     * @throws io.axual.ksml.data.exception.DataException when validation fails
     */
    @Override
    public void put(String key, Object value) {
        final var field = schema.field(key);

        if (field.schema() instanceof StructSchema structSchema && value instanceof Map)
            value = new AvroObject(structSchema, (Map<?, ?>) value);
        if (field.schema() instanceof EnumSchema)
            value = new GenericData.EnumSymbol(AVRO_SCHEMA_MAPPER.fromDataSchema(field.schema()), value != null ? value.toString() : null);

        final var fieldSchema = AVRO_SCHEMA_MAPPER.fromDataSchema(field.schema());
        if ((value != null || field.required()) && fieldSchema != null && !VALIDATOR.validate(fieldSchema, value))
            throw DataException.validationFailed(key, value);

        data.put(key, value);
    }

    /**
     * Get a field value by name.
     *
     * @param key field name
     * @return the previously assigned value (possibly a nested AvroObject or EnumSymbol), or null
     */
    @Override
    public Object get(String key) {
        return data.get(key);
    }

    /**
     * Put a field value by positional index.
     *
     * @param index zero-based index into the struct fields
     * @param value value to assign
     * @throws io.axual.ksml.data.exception.DataException when validation fails
     */
    @Override
    public void put(int index, Object value) {
        put(schema.fields().get(index).name(), value);
    }

    /**
     * Get a field value by positional index.
     *
     * @param index zero-based index into the struct fields
     * @return the value at the given index
     */
    @Override
    public Object get(int index) {
        return data.get(schema.fields().get(index).name());
    }

    /**
     * Lazily derive and cache the Avro Schema equivalent of this object's StructSchema.
     *
     * @return the Avro Schema used for validation and serialization
     */
    @Override
    public Schema getSchema() {
        if (avroSchema == null) avroSchema = AVRO_SCHEMA_MAPPER.fromDataSchema(schema);
        return avroSchema;
    }
}
