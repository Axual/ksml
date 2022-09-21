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

import java.util.List;
import java.util.Objects;

import io.axual.ksml.schema.RecordSchema;

// First attempt at providing an internal schema class. The implementation relies heavily on Avro
// at the moment, which is fine for now, but may change in the future.
public class AvroSchema extends RecordSchema {
    private final Schema avroSchema;

    public AvroSchema(Schema schema) {
        super(schema.getNamespace(), schema.getName(), schema.getDoc(), AvroUtil.convertFieldsFromAvro(schema.getFields()));
        avroSchema = schema;
    }

    public AvroSchema(RecordSchema schema) {
        super(schema.namespace(), schema.name(), schema.doc(), schema.fields());
        avroSchema = generateSchema();
    }

    public Schema schema() {
        return avroSchema;
    }

    private Schema generateSchema() {
        List<Schema.Field> fields = AvroUtil.convertFieldsToAvro(fields());
        return Schema.createRecord(name(), doc(), namespace(), false, fields);
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        var otherSchema = ((AvroSchema) other).schema();
        return schema().equals(otherSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generateSchema(), super.hashCode());
    }
}
