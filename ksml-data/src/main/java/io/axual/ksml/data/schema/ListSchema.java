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
import lombok.NonNull;

/**
 * A schema representation for lists in the KSML framework.
 * <p>
 * The {@code ListSchema} class extends {@link DataSchema} and is used to define
 * the structure of lists within the schema system. Each list is composed of
 * elements that share the same schema, defined by the {@code valueSchema}.
 * </p>
 * <p>
 * This schema is particularly useful for modeling collections or arrays of
 * homogeneous data types.
 * </p>
 */
@EqualsAndHashCode
public class ListSchema extends DataSchema {
    /**
     * The name of this list schema. Only sporadically necessary, eg. for XML schema where a field may
     * refer to a list type by name.
     */
    private final String name;
    /**
     * The schema of the elements contained in the list.
     * <p>
     * All elements in the list must adhere to this schema, ensuring type
     * consistency within the collection.
     * </p>
     */
    @Getter
    private final DataSchema valueSchema;

    /**
     * Constructs a {@code ListSchema} with a given element schema.
     *
     * @param valueSchema The {@link DataSchema} that defines the structure of each element in the list.
     *                    It must not be {@code null}.
     */
    public ListSchema(@NonNull DataSchema valueSchema) {
        super(DataSchemaConstants.LIST_TYPE);
        this.name = null;
        this.valueSchema = valueSchema;
    }

    /**
     * Constructs a {@code ListSchema} with a given element schema.
     *
     * @param valueSchema The {@link DataSchema} that defines the structure of each element in the list.
     *                    It must not be {@code null}.
     */
    public ListSchema(String name, @NonNull DataSchema valueSchema) {
        super(DataSchemaConstants.LIST_TYPE);
        this.name = name;
        this.valueSchema = valueSchema;
    }

    /**
     * Checks whether this schema has a name.
     *
     * @return {@code true} if the {@code name} field is not {@code null} and not empty;
     * {@code false} otherwise.
     */
    public boolean hasName() {
        return name != null && !name.isEmpty();
    }

    /**
     * Returns the name of this schema, or a default name if the name is not defined.
     *
     * @return {@code name} if it is defined, or a default name otherwise.
     */
    public String name() {
        if (hasName()) return name;
        return "Anonymous" + getClass().getSimpleName();
    }

    /**
     * Determines if this schema can be assigned from another schema.
     * <p>
     * This method checks whether the provided {@code otherSchema} is compatible
     * with this {@code ListSchema}. Compatibility for a {@code ListSchema} means that
     * the other schema is also a list and its value schema is compatible
     * with this schema's {@code valueSchema}.
     * </p>
     *
     * @param otherSchema The other {@link DataSchema} to be checked for compatibility.
     * @return {@code true} if the other schema is assignable from this schema;
     * {@code false} otherwise.
     */
    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof ListSchema otherListSchema)) return false;
        // If the value schema is null, then any schema can be assigned.
        if (valueSchema == null) return true;
        // This schema is assignable from the other schema when the value schema is assignable from
        // the otherSchema's value schema.
        return valueSchema.isAssignableFrom(otherListSchema.valueSchema);
    }

    /**
     * Returns a string representation of this schema, providing the type name as a string.
     *
     * @return A string representing the schema type.
     */
    @Override
    public String toString() {
        return super.toString() + " of " + valueSchema;
    }
}
