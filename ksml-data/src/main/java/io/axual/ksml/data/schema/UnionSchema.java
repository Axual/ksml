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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

/**
 * Represents a union schema that allows for multiple possible types in the KSML framework.
 * <p>
 * The {@code UnionSchema} class extends {@link DataSchema} and is used to define a schema
 * that can represent multiple different data types. A union schema is often used in cases where
 * the data can belong to one of several types, enabling flexible and dynamic data structures.
 * </p>
 * <p>
 * This class maintains an ordered collection of possible schemas (types) that the union can represent.
 * Each schema is unique within the union and is also mapped for fast access.
 * </p>
 */
@Getter
@EqualsAndHashCode
public class UnionSchema extends DataSchema {
    /**
     * The list of possible schemas (types) that this union schema can represent. The
     * types are stored as DataFields to accommodate for schema types like Protobuf,
     * where we need to keep track of field indices.
     * <p>
     * The schemas are stored in the order they are specified, and each schema represents
     * one possible type for the data.
     * </p>
     */
    private final DataField[] memberSchemas;

    /**
     * Constructs a {@code UnionSchema} with the given member schemas.
     *
     * @param memberSchemas A list of {@link DataSchema} representing the types that this union schema can adopt.
     *                      It must not be null or empty, and each schema must have a unique type.
     * @throws IllegalArgumentException if {@code memberSchemas} is null, empty, or contains duplicate or null schemas.
     */
    public UnionSchema(DataField... memberSchemas) {
        this(true, memberSchemas);
    }

    // Optimize
    // Make this public when the need arises to manually control optimization
    private UnionSchema(boolean flatten, DataField... memberSchemas) {
        super(DataSchemaConstants.UNION_TYPE);
        this.memberSchemas = flatten ? recursivelyGetMemberSchemas(memberSchemas) : memberSchemas;
    }

    private DataField[] recursivelyGetMemberSchemas(DataField[] memberSchemas) {
        // Here we flatten the list of value types by recursively walking through all value types. Any sub-unions
        // are exploded and taken up in this union's list of value types.
        final var result = new ArrayList<DataField>();
        for (final var memberSchema : memberSchemas) {
            if (memberSchema.schema() instanceof UnionSchema unionSchema) {
                final var subFields = recursivelyGetMemberSchemas(unionSchema.memberSchemas);
                result.addAll(Arrays.stream(subFields).toList());
            } else {
                result.add(memberSchema);
            }
        }
        return result.toArray(new DataField[]{});
    }

    /**
     * Determines if this union holds a specific member schema.
     *
     * @param schema The {@link DataSchema} to check for.
     * @return {@code true} if the schema is member of this union schema, {@code false} otherwise.
     */
    public boolean contains(DataSchema schema) {
        for (final var memberSchema : memberSchemas) {
            if (memberSchema.schema().equals(schema)) return true;
        }
        return false;
    }

    /**
     * Determines if this schema can be assigned from another schema.
     * <p>
     * A schema is assignable to this union schema if it matches any of the member schemas in this union.
     * </p>
     *
     * @param otherSchema The {@link DataSchema} to check for compatibility.
     * @return {@code true} if the other schema is assignable to this union schema, {@code false} otherwise.
     */
    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        // Don't call the super method here, since that gives wrong semantics. As a union we are
        // assignable from any schema type, so we must skip the comparison of our own schema type
        // with that of the other schema.

        // By convention, we are not assignable if the other schema is null.
        if (otherSchema == null) return false;

        // If the other schema is a union, then we compare all value types of that union.
        if (otherSchema instanceof UnionSchema otherUnionSchema) {
            // This schema is assignable from the other union fields when all of its value types can be assigned to
            // this union.
            for (final var otherUnionMemberSchema : otherUnionSchema.memberSchemas) {
                if (!isAssignableFrom(otherUnionMemberSchema))
                    return false;
            }
            return true;
        }

        // The other schema is not a union --> we are assignable from the other schema if at least
        // one of our value schema is assignable from the other schema.
        for (final var memberSchema : memberSchemas) {
            if (memberSchema.schema().isAssignableFrom(otherSchema)) return true;
        }
        return false;
    }

    private boolean isAssignableFrom(DataField otherField) {
        for (final var memberSchema : memberSchemas) {
            // First check if the schema of this field and the other field are compatible
            if (memberSchema.schema().isAssignableFrom(otherField.schema())) {
                // If they are, then manually check if we allow assignment from the other field to this field
                if (isAssignableByNameAndTag(memberSchema, otherField)) return true;
            }
        }
        return false;
    }

    private boolean isAssignableByNameAndTag(DataField thisField, DataField otherField) {
        // Allow assignments from an anonymous union type, having name or tag unset
        if (thisField.name() == null || otherField.name() == null) return true;
        if (thisField.tag() == NO_TAG || otherField.tag() == NO_TAG) return true;
        // This code is specifically made for PROTOBUF oneOf types, containing a field name and tag. We allow
        // assignment only if both fields match.
        if (!Objects.equals(thisField.name(), otherField.name())) return false;
        return Objects.equals(thisField.tag(), otherField.tag());
    }
}
