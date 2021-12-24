package io.axual.ksml.data.type.user;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import java.util.Objects;

import io.axual.ksml.data.type.base.ComplexType;
import io.axual.ksml.data.type.base.DataType;

public abstract class ComplexUserType implements UserType {
    private final DataType type;
    private final String notation;
    private final UserType[] subTypes;

    ComplexUserType(ComplexType type, String notation, UserType... subTypes) {
        this.type = type;
        this.notation = notation;
        this.subTypes = subTypes;
    }

    protected static DataType[] convertTypes(UserType... subTypes) {
        DataType[] result = new DataType[subTypes.length];
        for (int index = 0; index < subTypes.length; index++) {
            result[index] = subTypes[index].type();
        }
        return result;
    }

    @Override
    public String toString() {
        var subTypeStr = new StringBuilder();
        for (UserType subType : subTypes) {
            subTypeStr.append(subTypeStr.length() > 0 ? ", " : "").append(subType);
        }
        return type.containerClass().getSimpleName() + "<" + subTypeStr + ">";
    }

    public DataType type() {
        return type;
    }

    public String notation() {
        return notation;
    }

    public int subTypeCount() {
        return subTypes.length;
    }

    public UserType subType(int index) {
        return subTypes[index];
    }

    protected String schemaName(String baseName) {
        return schemaName(baseName, "Of");
    }

    protected String schemaName(String baseName, String midString) {
        return schemaName(baseName, midString, "And");
    }

    protected String schemaName(String baseName, String midString, String subTypeConcatenation) {
        StringBuilder builder = new StringBuilder(baseName);
        if (subTypes.length > 0) {
            builder.append(midString);
            for (int index = 0; index < subTypes.length; index++) {
                if (index > 0) builder.append(subTypeConcatenation);
                builder.append(subTypes[index].type().schemaName());
            }
        }
        return builder.toString();
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ComplexUserType other = (ComplexUserType) obj;
        if (!Objects.equals(notation, other.notation)) return false;
        if (!type().isAssignableFrom(other.type())) return false;
        return other.type().isAssignableFrom(type());
    }

    public int hashCode() {
        int result = super.hashCode();
        result = result * 31 + type.hashCode();
        for (var subType : subTypes) {
            result = result * 31 + subType.hashCode();
        }
        return result;
    }
}
