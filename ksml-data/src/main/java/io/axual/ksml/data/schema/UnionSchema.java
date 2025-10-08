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

import io.axual.ksml.data.compare.Compared;
import io.axual.ksml.data.compare.Equals;
import io.axual.ksml.data.type.Flags;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_SCHEMA_MEMBERS;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_SCHEMA_MEMBER_NAME;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_SCHEMA_MEMBER_SCHEMA;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_SCHEMA_MEMBER_TAG;

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
    // Definition of a union member
    public record Member(String name, DataSchema schema, int tag) implements Equals {
        public Member(DataSchema schema) {
            this(null, schema, NO_TAG);
        }

        @Override
        public Compared equals(Object obj, Flags flags) {
            if (this == obj) return Compared.ok();
            if (obj == null) return Compared.otherIsNull(this);
            if (!getClass().equals(obj.getClass())) return Compared.notEqual(getClass(), obj.getClass());

            final var that = (Member) obj;

            // Compare name
            if (!flags.isSet(IGNORE_UNION_SCHEMA_MEMBER_NAME) && !Objects.equals(name, that.name))
                return Compared.fieldNotEqual("name", this, name, that, that.name);

            // Compare schema
            if (!flags.isSet(IGNORE_UNION_SCHEMA_MEMBER_SCHEMA)) {
                final var schemaCompared = schema.equals(that.schema, flags);
                if (schemaCompared.isError())
                    return Compared.fieldNotEqual("schema", this, schema, that, that.schema, schemaCompared);
            }

            // Compare tag
            if (!flags.isSet(IGNORE_UNION_SCHEMA_MEMBER_TAG) && !Objects.equals(tag, that.tag))
                return Compared.fieldNotEqual("tag", this, tag, that, that.tag);

            return Compared.ok();
        }

        public String toString() {
            return "Member(name=" + name + ", schema=" + schema + ", tag=" + tag + ")";
        }
    }

    /**
     * The list of possible schemas (types) that this union schema can represent. The
     * types are stored as DataFields to accommodate for schema types like Protobuf,
     * where we need to keep track of field indices.
     * <p>
     * The schemas are stored in the order they are specified, and each schema represents
     * one possible type for the data.
     * </p>
     */
    private final Member[] members;

    /**
     * Constructs a {@code UnionSchema} with the given members.
     *
     * @param members A list of {@link Member}s representing the types that this union schema can adopt.
     *                It must not be null or empty, and each schema must have a unique type.
     * @throws IllegalArgumentException if {@code memberSchemas} is null, empty, or contains duplicate or null schemas.
     */
    public UnionSchema(Member... members) {
        this(true, members);
    }

    // Optimize
    // Make this public when the need arises to manually control optimization
    private UnionSchema(boolean flatten, Member... members) {
        super(DataSchemaConstants.UNION_TYPE);
        this.members = flatten ? recursivelyGetMembers(members) : members;
    }

    private Member[] recursivelyGetMembers(Member[] members) {
        // Here we flatten the list of value types by recursively walking through all value types. Any sub-unions
        // are exploded and taken up in this union's list of value types.
        final var result = new ArrayList<Member>();
        for (final var member : members) {
            if (member.schema() instanceof UnionSchema unionSchema) {
                final var subMembers = recursivelyGetMembers(unionSchema.members);
                result.addAll(Arrays.stream(subMembers).toList());
            } else {
                result.add(member);
            }
        }
        return result.toArray(Member[]::new);
    }

    /**
     * Determines if this union holds a specific member schema.
     *
     * @param schema The {@link DataSchema} to check for.
     * @return {@code true} if the schema is member of this union schema, {@code false} otherwise.
     */
    public boolean contains(DataSchema schema) {
        for (final var member : members) {
            if (member.schema().equals(schema)) return true;
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
     */
    @Override
    public Compared checkAssignableFrom(DataSchema otherSchema) {
        // Don't call the super method here, since that gives wrong semantics. As a union we are
        // assignable from any schema type, so we must skip the comparison of our own schema type
        // with that of the other schema.

        // By convention, we are not assignable if the other schema is null.
        if (otherSchema == null) {
            return Compared.error("Union schema is not assignable from null schema");
        }

        // If the other schema is a union, then we compare all value types of that union.
        if (otherSchema instanceof UnionSchema otherUnionSchema) {
            // This schema is assignable from the other union fields when all of its value types can be assigned to
            // this union.
            for (final var otherUnionMember : otherUnionSchema.members) {
                if (!checkAssignableFromMember(otherUnionMember).isOK())
                    return Compared.schemaMismatch(this, otherUnionMember.schema);
            }
            return Compared.ok();
        }

        // The other schema is not a union --> we are assignable from the other schema if at least
        // one of our value schema is assignable from the other schema.
        for (final var memberSchema : members) {
            if (memberSchema.schema().checkAssignableFrom(otherSchema).isOK()) return Compared.ok();
        }
        return Compared.schemaMismatch(this, otherSchema);
    }

    private Compared checkAssignableFromMember(Member otherMember) {
        for (final var member : members) {
            // First, check if the schema of this field and the other field are compatible
            if (member.schema().checkAssignableFrom(otherMember.schema()).isOK()
                    // If they are, then manually check if we allow assignment from the other field to this field
                    && isAssignableByNameAndTag(member, otherMember))
                return Compared.ok();
        }
        return Compared.schemaMismatch(this, otherMember.schema);
    }

    private boolean isAssignableByNameAndTag(Member thisField, Member otherField) {
        // Allow assignments from an anonymous union type, having name or tag unset
        if (thisField.name() == null || otherField.name() == null) return true;
        if (thisField.tag() == NO_TAG || otherField.tag() == NO_TAG) return true;
        // This code is specifically made for PROTOBUF oneOf types, containing a field name and tag. We allow
        // assignment only if both fields match.
        if (!Objects.equals(thisField.name(), otherField.name())) return false;
        return Objects.equals(thisField.tag(), otherField.tag());
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Compared equals(Object other, Flags flags) {
        final var superVerified = super.equals(other, flags);
        if (superVerified.isError()) return superVerified;

        final var that = (UnionSchema) other;

        // Compare members
        if (!flags.isSet(IGNORE_UNION_SCHEMA_MEMBERS)) {
            if (members.length != that.members.length)
                return Compared.fieldNotEqual("memberCount", this, members.length, that, that.members.length);

            for (int i = 0; i < members.length; i++) {
                final var memberCompared = members[i].equals(that.members[i], flags);
                if (memberCompared.isError())
                    return Compared.fieldNotEqual("member[" + i + "]", this, members[i], that, that.members[i], memberCompared);
            }
        }

        return Compared.ok();
    }
}
