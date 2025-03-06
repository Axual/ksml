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

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Represents a generic internal schema definition, capable of handling various schema types.
 * Instances of this class are used to define and interact with supported data schemas.
 */
@Getter
@EqualsAndHashCode
public class DataSchema {
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
     * The {@link DataSchema} instance representing ANY schema.
     */
    public static final DataSchema ANY_SCHEMA = new DataSchema(DataSchemaConstants.ANY_TYPE) {
        @Override
        public boolean isAssignableFrom(DataSchema otherSchema) {
            // This schema is assumed to be assignable from any other schema.
            return otherSchema != null;
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
    public static final DataSchema BYTE_SCHEMA = new DataSchema(DataSchemaConstants.BYTE_TYPE);
    /**
     * The {@link DataSchema} instance representing a SHORT schema.
     */
    public static final DataSchema SHORT_SCHEMA = new DataSchema(DataSchemaConstants.SHORT_TYPE);
    /**
     * The {@link DataSchema} instance representing a INTEGER schema.
     */
    public static final DataSchema INTEGER_SCHEMA = new DataSchema(DataSchemaConstants.INTEGER_TYPE);
    /**
     * The {@link DataSchema} instance representing a LONG schema.
     */
    public static final DataSchema LONG_SCHEMA = new DataSchema(DataSchemaConstants.LONG_TYPE);
    /**
     * The {@link DataSchema} instance representing a DOUBLE schema.
     */
    public static final DataSchema DOUBLE_SCHEMA = new DataSchema(DataSchemaConstants.DOUBLE_TYPE);
    /**
     * The {@link DataSchema} instance representing a FLOAT schema.
     */
    public static final DataSchema FLOAT_SCHEMA = new DataSchema(DataSchemaConstants.FLOAT_TYPE);
    /**
     * The {@link DataSchema} instance representing a BYTES schema.
     */
    public static final DataSchema BYTES_SCHEMA = new DataSchema(DataSchemaConstants.BYTES_TYPE);
    /**
     * The {@link DataSchema} instance representing a STRING schema.
     */
    public static final DataSchema STRING_SCHEMA = new DataSchema(DataSchemaConstants.STRING_TYPE) {
        @Override
        public boolean isAssignableFrom(DataSchema otherSchema) {
            if (otherSchema == NULL_SCHEMA) return true; // Allow assigning from NULL values
            if (otherSchema.type.equals(DataSchemaConstants.ENUM_TYPE)) return true; // Allow assigning from ENUM values
            return super.isAssignableFrom(otherSchema);
        }
    };

    /**
     * Checks if this schema type is assignable from another schema type.
     * This means the other schema's type can be safely used in place of this schema's type.
     *
     * @param otherSchema The other schema to compare.
     * @return {@code true} if this schema type can be assigned from the other schema, {@code false} otherwise.
     */
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (otherSchema == null) return false;
        return type.equals(otherSchema.type); // Base scenario: compare types and return true if similar
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
