package io.axual.ksml.schema;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.axual.ksml.notation.AvroNotation;

// First attempt at providing an internal schema class. The implementation relies heavily on Avro
// at the moment, which is fine for now, but may change in the future.
public class DataSchema {
    private final Schema schema;
    private final String schemaStr;

    private DataSchema(Schema schema) {
        this.schema = schema;
        this.schemaStr = schema.toString();
        SchemaLibrary.registerSchema(this);
    }

    protected Schema schema() {
        return schema;
    }

    public String name() {
        return schema.getFullName();
    }

    public String notation() {
        return AvroNotation.NAME;
    }

    @Override
    public String toString() {
        return schemaStr;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || other.getClass() != getClass()) return false;
        var otherSchema = (DataSchema) other;
        if (schema == null) return otherSchema.schema == null;
        return schema.equals(otherSchema.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    public static Builder newBuilder(Schema schema) {
        return new Builder(schema);
    }

    public static Builder newBuilder(String name, String doc, String namespace) {
        return new Builder(name, doc, namespace);
    }

    public static class Builder {
        private final Schema schema;
        private final List<Schema.Field> fields = new ArrayList<>();

        public Builder(Schema schema) {
            this.schema = schema;
        }

        public Builder(String name, String doc, String namespace) {
            schema = Schema.createRecord(name, doc, namespace, false);
        }

        public Builder addField(Schema.Field field) {
            fields.add(field);
            return this;
        }

        public Builder addField(String name, Schema schema, String doc, Object defaultValue) {
            addField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
            return this;
        }

        public Builder addField(String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
            addField(new Schema.Field(name, schema, doc, defaultValue, order));
            return this;
        }

        public DataSchema build() {
            if (!fields.isEmpty()) {
                schema.setFields(fields);
            }
            return new DataSchema(schema);
        }
    }
}
