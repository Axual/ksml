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

import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_FIXED_SCHEMA_SIZE;
import static io.axual.ksml.data.util.AssignableUtil.schemaMismatch;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;

/**
 * A schema representation for fixed-size binary data in the KSML framework.
 * <p>
 * The {@code FixedSchema} class extends the {@link NamedSchema} and is used to define
 * schemas for fixed-size binary data. It provides a {@code size} attribute that specifies
 * the exact length of the binary data.
 * </p>
 * <p>
 * This schema is useful for scenarios where the binary data must always conform to a
 * specific size, such as for serialization or protocol definitions.
 * </p>
 */
@Getter
@EqualsAndHashCode
public class FixedSchema extends NamedSchema {
    /**
     * The fixed size (in bytes) of the binary data represented by this schema.
     * <p>
     * This value is a positive integer, and it must be explicitly defined at
     * the time of schema creation.
     * </p>
     */
    private final int size;

    /**
     * Constructs a {@code FixedSchema} with the given namespace, name, documentation,
     * and size.
     *
     * @param namespace The namespace of this schema, typically used to avoid name collisions.
     * @param name      The name of the fixed schema.
     * @param doc       A brief description or documentation for this schema.
     * @param size      The fixed size (in bytes) for the binary data represented by this schema.
     *                  This must be a positive integer.
     * @throws IllegalArgumentException if the {@code size} is less than 0.
     */
    public FixedSchema(String namespace, String name, String doc, int size) {
        super(DataSchemaConstants.FIXED_TYPE, namespace, name, doc);
        if (size < 0) {
            throw new IllegalArgumentException("Size of FIXED type can not be smaller than zero. Found " + size);
        }
        this.size = size;
    }

    /**
     * Determines if this schema can be assigned from another schema.
     * <p>
     * This method checks whether the provided {@code otherSchema} is compatible
     * with this {@code FixedSchema}. Compatibility typically means that the other schema
     * has the same fixed size and similar characteristics.
     * </p>
     *
     * @param otherSchema The other {@link DataSchema} to be checked for compatibility.
     */
    @Override
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        final var superAssignable = super.isAssignableFrom(otherSchema);
        if (superAssignable.isError()) return superAssignable;
        if (!(otherSchema instanceof FixedSchema otherFixedSchema)) return schemaMismatch(this, otherSchema);
        if (size >= otherFixedSchema.size) return Assignable.ok();
        return Assignable.error("Size of fixed schema (" + size + ") is smaller than the other fixed schema's size (" + otherFixedSchema.size + ")");
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

        final var that = (FixedSchema) obj;

        // Compare size
        if (!flags.isSet(IGNORE_FIXED_SCHEMA_SIZE) && !Objects.equals(size, that.size))
            return fieldNotEqual("size", this, size, that, that.size);

        return Equal.ok();
    }
}
