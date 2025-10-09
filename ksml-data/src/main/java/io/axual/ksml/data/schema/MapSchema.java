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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_MAP_SCHEMA_VALUE_SCHEMA;
import static io.axual.ksml.data.util.AssignableUtil.fieldNotAssignable;
import static io.axual.ksml.data.util.AssignableUtil.schemaMismatch;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;

/**
 * A schema representation for maps in the KSML framework.
 * <p>
 * The {@code MapSchema} class extends {@link DataSchema} and is designed to define
 * the structure of maps (key-value pairs) within the schema system. Each map is composed of
 * keys and values, where the key must always be a string, and the {@code valueSchema}
 * defines the structure of the map's values.
 * </p>
 * <p>
 * This schema is commonly used in scenarios where a key-value collection needs
 * to be represented with specific constraints on the value types.
 * </p>
 */
@Getter
@EqualsAndHashCode
public class MapSchema extends DataSchema {
    /**
     * The schema of the values contained in the map.
     * <p>
     * All values in the map must adhere to this schema, ensuring type consistency across
     * the map's contents.
     * </p>
     */
    private final DataSchema valueSchema;

    /**
     * Constructs a {@code MapSchema} with a given value schema.
     *
     * @param valueSchema The {@link DataSchema} that defines the structure of the map's values.
     *                    It must not be {@code null}.
     * @throws IllegalArgumentException if the {@code valueSchema} is {@code null}.
     */
    public MapSchema(@NonNull DataSchema valueSchema) {
        super(DataSchemaConstants.MAP_TYPE);
        this.valueSchema = valueSchema;
    }

    /**
     * Determines if this schema can be assigned from another schema.
     * <p>
     * This method checks whether the provided {@code otherSchema} is compatible
     * with this {@code MapSchema}. Compatibility for a {@code MapSchema} means that
     * the other schema is also a map, and its value schema is compatible with this
     * schema's {@code valueSchema}.
     * </p>
     *
     * @param otherSchema The other {@link DataSchema} to be checked for compatibility.
     */
    @Override
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        final var superAssignable = super.isAssignableFrom(otherSchema);
        if (superAssignable.isError()) return superAssignable;
        if (!(otherSchema instanceof MapSchema that)) return schemaMismatch(this, otherSchema);
        // This schema is assignable from the other schema when the value schema is assignable from
        // the otherSchema's value schema.
        final var valueSchemaAssignable = valueSchema.isAssignableFrom(that.valueSchema);
        if (valueSchemaAssignable.isError())
            return fieldNotAssignable("valueSchema", this, valueSchema, that, that.valueSchema, valueSchemaAssignable);
        return Assignable.ok();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param obj   The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equal equals(Object obj, Flags flags) {
        final var superEqual = super.equals(obj, flags);
        if (superEqual.isError()) return superEqual;

        final var that = (MapSchema) obj;

        // Compare valueSchema
        if (!flags.isSet(IGNORE_MAP_SCHEMA_VALUE_SCHEMA)) {
            final var valueSchemaEqual = valueSchema.equals(that.valueSchema, flags);
            if (valueSchemaEqual.isError())
                return fieldNotEqual("valueSchema", this, valueSchema, that, that.valueSchema, valueSchemaEqual);
        }

        return Equal.ok();
    }
}
