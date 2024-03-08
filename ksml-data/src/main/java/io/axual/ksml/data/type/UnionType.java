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

import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

@Getter
public class UnionType extends ComplexType {
    private static final String UNION_NAME = "Union";
    private final DataType[] possibleTypes;

    public UnionType(DataType... possibleTypes) {
        super(Object.class, userTypesToDataTypes(possibleTypes));
        this.possibleTypes = possibleTypes;
    }

    private static DataType[] userTypesToDataTypes(DataType... userTypes) {
        var dataTypes = new DataType[userTypes.length];
        System.arraycopy(userTypes, 0, dataTypes, 0, userTypes.length);
        return dataTypes;
    }

    public DataType[] possibleTypes() {
        return possibleTypes;
    }

    @Override
    public String containerName() {
        return UNION_NAME;
    }

    @Override
    public String schemaName() {
        return schemaName(UNION_NAME, "Of", "Or");
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        if (this == type) return true;

        // If the other dataType is a union, then compare the union with this dataType
        if (type instanceof UnionType otherUnion && equalsOtherUnion(otherUnion)) return true;

        // If the union did not match in its entirety, then check for assignable subtypes
        for (var possibleType : possibleTypes) {
            if (possibleType.isAssignableFrom(type)) return true;
        }
        return false;
    }

    private boolean equalsOtherUnion(UnionType other) {
        var otherPossibleTypes = other.possibleTypes();
        if (possibleTypes.length != otherPossibleTypes.length) return false;
        for (int index = 0; index < possibleTypes.length; index++) {
            if (!possibleTypes[index].isAssignableFrom(otherPossibleTypes[index]))
                return false;
            if (!otherPossibleTypes[index].isAssignableFrom(possibleTypes[index]))
                return false;
        }
        return true;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        for (var possibleType : possibleTypes) {
            if (possibleType.isAssignableFrom(value)) return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        return equalsOtherUnion((UnionType) other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(possibleTypes));
    }
}
