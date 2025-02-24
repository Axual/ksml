package io.axual.ksml.data.type;

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

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.schema.StructSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class StructType extends MapType {
    private static final String DEFAULT_NAME = "Struct";
    private final String name;
    private final StructSchema schema;

    public StructType() {
        this(null, null);
    }

    public StructType(StructSchema schema) {
        this(null, schema);
    }

    public StructType(String name) {
        this(name, null);
    }

    private StructType(String name, StructSchema schema) {
        super(UNKNOWN);
        this.name = name != null && !name.isEmpty() ? name : schema != null ? schema.name() : DEFAULT_NAME;
        this.schema = schema;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String name() {
        return schema != null ? schema.name() : name;
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        if (type== DataNull.DATATYPE) return true; // Always allow Structs to be NULL (Kafka tombstones)
        if (!super.isAssignableFrom(type)) return false;
        if (!(type instanceof StructType structType)) return false;
        if (schema == null) return true;
        return schema.isAssignableFrom(structType.schema);
    }
}
