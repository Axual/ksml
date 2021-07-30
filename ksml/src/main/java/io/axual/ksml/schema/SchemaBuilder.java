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

public class SchemaBuilder {
    private final Schema schema;
    private final List<Schema.Field> fields = new ArrayList<>();

    public SchemaBuilder(Schema schema) {
        this.schema = schema;
    }

    public SchemaBuilder(String name, String doc, String namespace) {
        schema = Schema.createRecord(name, doc, namespace, false);
    }

    public SchemaBuilder addField(Schema.Field field) {
        fields.add(field);
        return this;
    }

    public SchemaBuilder addField(String name, Schema schema, String doc, Object defaultValue) {
        addField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
        return this;
    }

    public SchemaBuilder addField(String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
        addField(new Schema.Field(name, schema, doc, defaultValue, order));
        return this;
    }

    public Schema build() {
        if (!fields.isEmpty()) {
            schema.setFields(fields);
        }
        return schema;
    }
}
