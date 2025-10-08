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
import io.axual.ksml.data.compare.Equals;
import io.axual.ksml.data.schema.DataSchemaConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_TYPE_MEMBERS;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_TYPE_MEMBER_NAME;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_TYPE_MEMBER_TAG;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_UNION_TYPE_MEMBER_TYPE;

/**
 * A {@link ComplexType} representing a tagged union (sum type) composed of multiple member types.
 * <p>
 * Assignability succeeds when either:
 * - the other type is an equivalent union (member-wise assignable in both directions), or
 * - the value/type is assignable to at least one of the union's member types.
 * <p>
 * The nested {@link Member} record describes an individual member of the union.
 */
@EqualsAndHashCode
@Getter
public class UnionType extends ComplexType {
    private final Member[] members;

    // Definition of a union member
    public record Member(String name, DataType type, int tag) implements Equals {
        public Member(DataType type) {
            this(null, type, NO_TAG);
        }

        @Override
        public Compared equals(Object other, Flags flags) {
            if (this == other) return Compared.ok();
            if (other == null) return Compared.otherIsNull(this);
            if (!getClass().equals(other.getClass())) return Compared.notEqual(getClass(), other.getClass());

            final var that = (Member) other;

            // Compare name
            if (!flags.isSet(IGNORE_UNION_TYPE_MEMBER_NAME) && !Objects.equals(name, that.name))
                return Compared.fieldNotEqual("name", this, name, that, that.name);

            // Compare type
            if (!flags.isSet(IGNORE_UNION_TYPE_MEMBER_TYPE)) {
                final var typeCompared = type.equals(that.type, flags);
                if (typeCompared.isError())
                    return Compared.fieldNotEqual("type", this, type, that, that.type, typeCompared);
            }

            // Compare tag
            if (!flags.isSet(IGNORE_UNION_TYPE_MEMBER_TAG) && !Objects.equals(tag, that.tag))
                return Compared.fieldNotEqual("tag", this, tag, that, that.tag);

            return Compared.ok();
        }
    }

    public UnionType(Member... members) {
        super(Object.class,
                buildName("Union", "Of", "Or", memberTypesToDataTypes(members)),
                DataSchemaConstants.UNION_TYPE + "(" + buildSpec(memberTypesToDataTypes(members)) + ")",
                memberTypesToDataTypes(members));
        this.members = members;
    }

    private static DataType[] memberTypesToDataTypes(Member... memberTypes) {
        var result = new DataType[memberTypes.length];
        for (int index = 0; index < memberTypes.length; index++) {
            result[index] = memberTypes[index].type();
        }
        return result;
    }

    @Override
    public Compared checkAssignableFrom(DataType type) {
        if (this == type) return Compared.ok();

        // If the other type is a union, then compare the union with this dataType
        if (type instanceof UnionType otherUnion) {
            // Check that all this union's member types are assignable from the other union. That is the case if and
            // only if this union's member types are equal or a superset of the other union's member types. We can
            // check this by making sure that all member types of the other union are assignable from this union.
            for (final var otherMember : otherUnion.members) {
                if (!isAssignableFromMember(otherMember)) {
                    return Compared.error("Can not assign member type \"" + otherMember.type + "\" to union type \"" + this + "\".");
                }
            }
            // All members can be assigned from other members, so return no error
            return Compared.ok();
        } else {
            for (final var member : members) {
                if (member.type().checkAssignableFrom(type).isOK()) return Compared.ok();
            }
            return Compared.error("Can not assign type \"" + type + "\" to union type \"" + this + "\".");
        }
    }

    private boolean isAssignableFromMember(Member otherMember) {
        // Check if the type is assignable from this union
        for (final var member : members) {
            if (member.type.checkAssignableFrom(otherMember.type).isOK()) return true;
        }
        return false;
    }

    @Override
    public Compared checkAssignableFrom(Object value) {
        for (final var member : members) {
            if (member.type().checkAssignableFrom(value).isOK()) return Compared.ok();
        }
        return Compared.typeMismatch(this, value);
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

        final var that = (UnionType) other;

        // Compare members
        if (!flags.isSet(IGNORE_UNION_TYPE_MEMBERS)) {
            // Two unions are equal if their members are all equal
            if (members.length != that.members.length) return Compared.notEqual(this, that);
            for (int index = 0; index < members.length; index++) {
                final var memberCompared = members[index].equals(that.members[index], flags);
                if (memberCompared.isError()) return Compared.notEqual(this, that, memberCompared);
            }
        }

        return Compared.ok();
    }
}
