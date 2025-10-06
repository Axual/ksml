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

import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

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
     * @param context     The validation context.
     */
    @Override
    public ValidationResult checkAssignableFrom(DataSchema otherSchema, ValidationContext context) {
        if (!super.checkAssignableFrom(otherSchema, context).isOK()) return context;
        if (!(otherSchema instanceof MapSchema otherMapSchema)) return context.schemaMismatch(this, otherSchema);
        // This schema is assignable from the other schema when the value schema is assignable from
        // the otherSchema's value schema.
        return valueSchema.checkAssignableFrom(otherMapSchema.valueSchema, context);
    }
}
