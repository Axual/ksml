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

import com.google.common.base.Objects;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.StructSchema;
import lombok.Getter;

import java.util.Map;

/**
 * A {@link ComplexType} representing a structured map-like type that may be backed by a
 * {@link io.axual.ksml.data.schema.StructSchema}.
 * <p>
 * When a schema is provided, field types and assignability are validated against it. StructType
 * allows {@code null} values to support Kafka tombstones.
 */
@Getter
public class StructType extends ComplexType {
    private static final String DEFAULT_NAME = "Struct";
    private static final DataTypeDataSchemaMapper MAPPER = new DataTypeDataSchemaMapper();
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
        super(Map.class,
                buildName("Map", DataType.UNKNOWN),
                DataSchemaConstants.MAP_TYPE + "(" + buildSpec(DataType.UNKNOWN) + ")",
                DataString.DATATYPE,
                DataType.UNKNOWN);
        if (schema == StructSchema.SCHEMALESS) schema = null; // If we're SCHEMALESS, then nullify the schema here
        if( name != null && !name.isEmpty() ){
            this.name = name;
        }else if( schema != null ){
            this.name = schema.name();
        }else {
            this.name = DEFAULT_NAME;
        }
        this.schema = schema;
    }

    public DataType keyType() {
        return subType(0);
    }

    public DataType valueType() {
        return subType(1);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String name() {
        return schema != null ? schema.name() : name;
    }

    public DataType fieldType(String fieldName, DataType incaseNoSchema, DataType incaseNoSuchField) {
        if (schema == null) return incaseNoSchema;
        final var field = schema.field(fieldName);
        if (field == null) return incaseNoSuchField;
        return MAPPER.fromDataSchema(field.schema());
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        if (type == DataNull.DATATYPE) return true; // Always allow Structs to be NULL (Kafka tombstones)
        if (!super.isAssignableFrom(type)) return false;
        if (!(type instanceof StructType structType)) return false;
        if (schema == null) return true;
        return schema.isAssignableFrom(structType.schema);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;
        StructType that = (StructType) other;
        if(!Objects.equal(this.name, that.name)) return false;
        return this.isAssignableFrom(that) && that.isAssignableFrom(this);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), name, schema);
    }
}
