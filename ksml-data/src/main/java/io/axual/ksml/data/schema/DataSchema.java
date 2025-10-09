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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.type.Flags;
import io.axual.ksml.data.util.EqualsUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;
import java.util.Set;

import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_DATA_SCHEMA_TYPE;
import static io.axual.ksml.data.util.AssignableUtil.schemaMismatch;
import static io.axual.ksml.data.util.EqualsUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualsUtil.otherIsNull;

/**
 * Represents a generic internal schema definition, capable of handling various schema types.
 * Instances of this class are used to define and interact with supported data schemas.
 */
@Getter
@EqualsAndHashCode
public class DataSchema {
    private static final String NO_SCHEMA_SPECIFIED = "No schema specified, this is a bug in KSML";

    /**
     * The type of this schema.
     */
    private final String type;

    /**
     * Protected constructor for initializing the schema with a specific type.
     *
     * @param type The type of the schema. Cannot be null.
     */
    protected DataSchema(String type) {
        this.type = type;
    }

    /**
     * The set of schema types that represent whole numbers.
     */
    private static final Set<String> INTEGER_TYPES = Set.of(
            DataSchemaConstants.BYTE_TYPE,
            DataSchemaConstants.SHORT_TYPE,
            DataSchemaConstants.INTEGER_TYPE,
            DataSchemaConstants.LONG_TYPE);
    /**
     * The set of schema types that represent floating point numbers.
     */
    private static final Set<String> FLOATING_POINT_TYPES = Set.of(
            DataSchemaConstants.FLOAT_TYPE,
            DataSchemaConstants.DOUBLE_TYPE);
    /**
     * The {@link DataSchema} instance representing ANY schema.
     */
    public static final DataSchema ANY_SCHEMA = new DataSchema(DataSchemaConstants.ANY_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            if (otherSchema == null) return Assignable.error("No other schema provided");
            // This schema is assumed to be assignable from any other schema.
            return Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a NULL schema.
     */
    public static final DataSchema NULL_SCHEMA = new DataSchema(DataSchemaConstants.NULL_TYPE);
    /**
     * The {@link DataSchema} instance representing a BOOLEAN schema.
     */
    public static final DataSchema BOOLEAN_SCHEMA = new DataSchema(DataSchemaConstants.BOOLEAN_TYPE);
    /**
     * The {@link DataSchema} instance representing a BYTE schema.
     */
    public static final DataSchema BYTE_SCHEMA = new DataSchema(DataSchemaConstants.BYTE_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !INTEGER_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a SHORT schema.
     */
    public static final DataSchema SHORT_SCHEMA = new DataSchema(DataSchemaConstants.SHORT_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !INTEGER_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a INTEGER schema.
     */
    public static final DataSchema INTEGER_SCHEMA = new DataSchema(DataSchemaConstants.INTEGER_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !INTEGER_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a LONG schema.
     */
    public static final DataSchema LONG_SCHEMA = new DataSchema(DataSchemaConstants.LONG_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !INTEGER_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a DOUBLE schema.
     */
    public static final DataSchema DOUBLE_SCHEMA = new DataSchema(DataSchemaConstants.DOUBLE_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !FLOATING_POINT_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a FLOAT schema.
     */
    public static final DataSchema FLOAT_SCHEMA = new DataSchema(DataSchemaConstants.FLOAT_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            return !FLOATING_POINT_TYPES.contains(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
        }
    };
    /**
     * The {@link DataSchema} instance representing a BYTES schema.
     */
    public static final DataSchema BYTES_SCHEMA = new DataSchema(DataSchemaConstants.BYTES_TYPE);
    /**
     * The {@link DataSchema} instance representing a STRING schema.
     */
    public static final DataSchema STRING_SCHEMA = new DataSchema(DataSchemaConstants.STRING_TYPE) {
        @Override
        public Assignable isAssignableFrom(DataSchema otherSchema) {
            Objects.requireNonNull(otherSchema, NO_SCHEMA_SPECIFIED);
            if (otherSchema == NULL_SCHEMA || otherSchema.type.equals(DataSchemaConstants.ENUM_TYPE)) {
                // Always allow assignment from NULL and ENUM schemas
                return Assignable.ok();
            }
            return super.isAssignableFrom(otherSchema);
        }
    };

    /**
     * Checks if this schema type is assignable from another schema type.
     * This means the other schema's type can be safely used in place of this schema's type.
     *
     * @param otherSchema The other schema to compare.
     */
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        if (otherSchema == null) return Assignable.error("No other schema provided");
        // Base scenario: check assignability of types and return error if not assignable
        return !type.equals(otherSchema.type) ? schemaMismatch(this, otherSchema) : Assignable.ok();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    public Equal equals(Object other, Flags flags) {
        if (this == other) return Equal.ok();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass()))
            return EqualsUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (DataSchema) other;

        // Compare type
        if (!flags.isSet(IGNORE_DATA_SCHEMA_TYPE) && !Objects.equals(type, that.type))
            return fieldNotEqual("type", this, type, that, that.type);

        return Equal.ok();
    }

    /**
     * Returns a string representation of this schema, providing the type name as a string.
     *
     * @return A string representing the schema type.
     */
    @Override
    public String toString() {
        return type;
    }
}
