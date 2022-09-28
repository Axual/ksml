package io.axual.ksml.data.type;

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

import java.util.Objects;

import io.axual.ksml.schema.RecordSchema;

public class RecordType extends MapType {
    private final String name;
    private final RecordSchema schema;

    public RecordType() {
        this((String) null);
    }

    public RecordType(RecordSchema schema) {
        this.name = getName(schema.name());
        this.schema = schema;
    }

    public RecordType(String name) {
        this.name = getName(name);
        this.schema = null;
    }

    private String getName(String name) {
        return name != null && name.length() > 0 ? name : "Record";
    }

    @Override
    public String toString() {
        return name;
    }

    public RecordSchema schema() {
        return schema;
    }

    @Override
    public String schemaName() {
        return schema != null ? schema.name() : "";
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof RecordType otherRecord)) return false;
        return Objects.equals(name, otherRecord.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }
}
