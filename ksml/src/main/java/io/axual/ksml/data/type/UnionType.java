package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

public class UnionType extends ComplexType {
    private static final String UNION_NAME = "Union";
    private final UserType[] possibleTypes;

    public UnionType(UserType... possibleTypes) {
        super(Object.class, userTypesToDataTypes(possibleTypes));
        this.possibleTypes = possibleTypes;
    }

    private static DataType[] userTypesToDataTypes(UserType... userTypes) {
        var dataTypes = new DataType[userTypes.length];
        for (int index = 0; index < userTypes.length; index++)
            dataTypes[index] = userTypes[index].dataType();
        return dataTypes;
    }

    public UserType[] possibleTypes() {
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
        for (UserType possibleType : possibleTypes) {
            if (possibleType.dataType().isAssignableFrom(type)) return true;
        }
        return false;
    }

    private boolean equalsOtherUnion(UnionType other) {
        var otherPossibleTypes = other.possibleTypes();
        if (possibleTypes.length != otherPossibleTypes.length) return false;
        for (int index = 0; index < possibleTypes.length; index++) {
            if (!possibleTypes[index].dataType().isAssignableFrom(otherPossibleTypes[index].dataType()))
                return false;
            if (!otherPossibleTypes[index].dataType().isAssignableFrom(possibleTypes[index].dataType()))
                return false;
        }
        return true;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        for (UserType possibleType : possibleTypes) {
            if (possibleType.dataType().isAssignableFrom(value)) return true;
        }
        return false;
    }
}
