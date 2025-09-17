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
 * The nested {@link MemberType} record describes an individual member of the union.
 */
@Getter
public class UnionType extends ComplexType {
    private final MemberType[] memberTypes;

    // A field type
    public record MemberType(String name, DataType type, int tag) {
        public MemberType(DataType type) {
            this(null, type, NO_TAG);
        }
    }

    public UnionType(MemberType... memberTypes) {
        super(Object.class,
                buildName("Union", "Of", "Or", memberTypesToDataTypes(memberTypes)),
                DataSchemaConstants.UNION_TYPE + "(" + buildSpec(memberTypesToDataTypes(memberTypes)) + ")",
                memberTypesToDataTypes(memberTypes));
        this.memberTypes = memberTypes;
    }

    private static DataType[] memberTypesToDataTypes(MemberType... memberTypes) {
        var result = new DataType[memberTypes.length];
        for (int index = 0; index < memberTypes.length; index++) {
            result[index] = memberTypes[index].type();
        }
        return result;
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        if (this == type) return true;

        // If the other dataType is a union, then compare the union with this dataType
        if (type instanceof UnionType otherUnion && isAssignableFromOtherUnion(otherUnion)) return true;

        // If the union did not match in its entirety, then check for assignable subtypes
        for (var memberType : memberTypes) {
            if (memberType.type().isAssignableFrom(type)) return true;
        }
        return false;
    }

    private boolean isAssignableFromOtherUnion(UnionType other) {
        var otherMemberTypes = other.memberTypes();
        if (memberTypes.length != otherMemberTypes.length) return false;
        for (int index = 0; index < memberTypes.length; index++) {
            if (!memberTypes[index].type().isAssignableFrom(otherMemberTypes[index].type()))
                return false;
            if (!otherMemberTypes[index].type().isAssignableFrom(memberTypes[index].type()))
                return false;
        }
        return true;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        for (final var memberType : memberTypes) {
            if (memberType.type().isAssignableFrom(value)) return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        return isAssignableFromOtherUnion((UnionType) other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(memberTypes));
    }
}
