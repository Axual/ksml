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

import io.axual.ksml.data.compare.Compared;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.StructSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

import static io.axual.ksml.data.type.EqualityFlags.IGNORE_STRUCT_TYPE_SCHEMA;

/**
 * A {@link ComplexType} representing a structured map-like type that may be backed by a
 * {@link io.axual.ksml.data.schema.StructSchema}.
 * <p>
 * When a schema is provided, field types and assignability are validated against it. StructType
 * allows {@code null} values to support Kafka tombstones.
 */
@EqualsAndHashCode
@Getter
public class StructType extends ComplexType {
    private static final String DEFAULT_NAME = "Struct";
    private static final DataTypeDataSchemaMapper MAPPER = new DataTypeDataSchemaMapper();
    private final StructSchema schema;

    public interface CompareFilter {
        default boolean ignoreTags() {
            return false;
        }
    }

    public StructType() {
        this(null);
    }

    public StructType(StructSchema schema) {
        super(Map.class,
                schema != null ? schema.name() : DEFAULT_NAME,
                DataSchemaConstants.STRUCT_TYPE,
                DataString.DATATYPE,
                DataType.UNKNOWN);
        if (schema == StructSchema.SCHEMALESS) schema = null; // If we're SCHEMALESS, then nullify the schema here
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
        return name();
    }

    public DataType fieldType(String fieldName, DataType incaseNoSchema, DataType incaseNoSuchField) {
        if (schema == null) return incaseNoSchema;
        final var field = schema.field(fieldName);
        if (field == null) return incaseNoSuchField;
        return MAPPER.fromDataSchema(field.schema());
    }

    @Override
    public Compared checkAssignableFrom(DataType otherType) {
        // Always allow Structs to be NULL (Kafka tombstones)
        if (otherType == DataNull.DATATYPE) return Compared.ok();
        // Perform superclass validation first
        final var superVerified = super.checkAssignableFrom(otherType);
        if (superVerified.isError()) return superVerified;
        if (!(otherType instanceof StructType otherStructType))
            return Compared.error("Type \"" + otherType + "\" is not a StructType");
        // In case we have no schema, then we can be assigned values from any other struct, with or without a schema
        if (schema == null) return Compared.ok();
        // When we have a schema, validate that the schema is assignable from the other struct's schema
        return schema.checkAssignableFrom(otherStructType.schema);
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Compared equals(Object other, Flags flags) {
        if (this == other) return Compared.ok();
        if (other == null) return Compared.otherIsNull(this);
        if (!getClass().equals(other.getClass())) return Compared.notEqual(getClass(), other.getClass());

        final var superCompared = super.equals(other, flags);
        if (superCompared.isError()) return superCompared;

        final var that = (StructType) other;

        // Compare schema
        if (!flags.isSet(IGNORE_STRUCT_TYPE_SCHEMA) && (schema != null || that.schema != null)) {
            if (schema == null || that.schema == null)
                return Compared.fieldNotEqual("schema", this, schema, that, that.schema);
            final var schemaCompared = schema.equals(that.schema, flags);
            return schemaCompared.isError() ? Compared.schemaMismatch(schema, that.schema, schemaCompared) : Compared.ok();
        }

        return Compared.ok();
    }
}
