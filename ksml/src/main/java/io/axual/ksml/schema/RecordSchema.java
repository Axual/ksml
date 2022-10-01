package io.axual.ksml.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RecordSchema extends NamedSchema {
    private final List<DataField> fields = new ArrayList<>();

    public RecordSchema(RecordSchema other) {
        this(other.namespace(), other.name(), other.doc(), other.fields);
    }

    public RecordSchema(String namespace, String name, String doc, List<DataField> fields) {
        super(Type.RECORD, namespace, name, doc);
        if (fields != null) {
            this.fields.addAll(fields);
        }
    }

    public int numFields() {
        return fields.size();
    }

    public DataField field(int index) {
        return fields.get(index);
    }

    public DataField field(String name) {
        for (DataField field : fields) {
            if (field.name().equals(name)) {
                return field;
            }
        }

        return null;
    }

    public List<DataField> fields() {
        return Lists.newCopyOnWriteArrayList(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return fields.equals(((RecordSchema) other).fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}
