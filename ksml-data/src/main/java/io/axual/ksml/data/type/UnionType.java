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

import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

/**
 * A {@link ComplexType} representing a tagged union (sum type) composed of multiple member types.
 * <p>
 * Assignability succeeds when either:
 * - the other type is an equivalent union (member-wise assignable in both directions), or
 * - the value/type is assignable to at least one of the union's member types.
 * <p>
 * The nested {@link Member} record describes an individual member of the union.
 */
@Getter
public class UnionType extends ComplexType {
    private final Member[] members;

    // Definition of a union member
    public record Member(String name, DataType type, int tag) {
        public Member(DataType type) {
            this(null, type, NO_TAG);
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
    public ValidationResult checkAssignableFrom(DataType type, ValidationContext context) {
        if (this == type) return context.ok();

        // If the other type is a union, then compare the union with this dataType
        if (type instanceof UnionType otherUnion) {
            // Check that all this union's member types are assignable from the other union. That is the case if and
            // only if this union's member types are equal or a superset of the other union's member types. We can
            // check this by making sure that all member types of the other union are assignable from this union.
            for (final var otherMember : otherUnion.members) {
                if (!isAssignableFromMember(otherMember)) {
                    context.addError("Can not assign member type \"" + context.thatType(otherMember.type) + "\" to union type \"" + context.thisType(this) + "\".");
                }
            }
            // All members can be assigned from other members, so return no error
            return context.ok();
        } else {
            for (final var member : members) {
                if (member.type().checkAssignableFrom(type).isOK()) return context.ok();
            }
            return context.addError("Can not assign type \"" + context.thatType(type) + "\" to union type \"" + context.thisType(this) + "\".");
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
    public ValidationResult checkAssignableFrom(Object value, ValidationContext context) {
        for (final var member : members) {
            if (member.type().checkAssignableFrom(value).isOK()) return context.ok();
        }
        return context.typeMismatch(this, value);
    }

    private boolean unionEquals(UnionType other) {
        // Two unions are equal if their members are all equal
        var otherMembers = other.members;
        if (members.length != otherMembers.length) return false;
        for (int index = 0; index < members.length; index++) {
            if (!members[index].equals(otherMembers[index])) return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof UnionType otherUnion)) return false;
        return unionEquals(otherUnion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(members));
    }
}
