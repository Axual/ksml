package io.axual.ksml.data.schema;

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

import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode
public class StructSchema extends NamedSchema {
    private final List<DataField> fields = new ArrayList<>();
    private final Map<String, DataField> fieldsByName = new HashMap<>();

    public StructSchema() {
        // This exists for compatibility reasons: if we define eg. JSON Objects, or schema-less Structs, then we need
        // to somehow capture this in a schema with the proper type. The StructSchema is the proper type, so we let
        // the absence of a schema be reflected through null fields.
        this(null, null, null, null);
    }

    public StructSchema(StructSchema other) {
        this(other.namespace(), other.name(), other.doc(), other.fields);
    }

    public StructSchema(String namespace, String name, String doc, List<DataField> fields) {
        super(Type.STRUCT, namespace, name, doc);
        if (fields != null) {
            this.fields.addAll(fields);
            for (var field : fields) {
                fieldsByName.put(field.name(), field);
            }
        }
    }

    public DataField field(int index) {
        return fields.get(index);
    }

    public DataField field(String name) {
        return fieldsByName.get(name);
    }

    public List<DataField> fields() {
        return Lists.newCopyOnWriteArrayList(fields);
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof StructSchema otherStructSchema)) return false;
        // This schema is assignable from the other schema when all fields without default values
        // are also found in the other schema
        for (var field : fields) {
            // Get the field from the other schema with the same name
            var otherField = otherStructSchema.field(field.name());
            // If the field exists in the other schema, then validate its compatibility
            if (otherField != null && !field.isAssignableFrom(otherField)) return false;
            // If this field has no default value, then the field should exist in the other schema
            if (field.defaultValue() == null && otherField == null) return false;
        }
        return true;
    }
}
