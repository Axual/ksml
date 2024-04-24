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
import java.util.List;
import java.util.Objects;

@Getter
public abstract class ComplexType implements DataType {
    private final Class<?> containerClass;
    private final DataType[] subTypes;

    public ComplexType(Class<?> containerClass, DataType... subTypes) {
        this.containerClass = containerClass;
        this.subTypes = subTypes;
    }

    @Override
    public String toString() {
        var subTypeStr = new StringBuilder();
        for (DataType subType : subTypes) {
            subTypeStr.append(subTypeStr.length() > 0 ? ", " : "").append(subType);
        }
        return containerName() + "<" + subTypeStr + ">";
    }

    public String containerName() {
        return containerClass.getSimpleName();
    }

    public Class<?> containerClass() {
        return containerClass;
    }

    public int subTypeCount() {
        return subTypes.length;
    }

    public DataType subType(int index) {
        return subTypes[index];
    }

    public List<DataType> subTypes() {
        return List.of(subTypes);
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
                builder.append(subTypes[index].schemaName());
            }
        }
        return builder.toString();
    }

    @Override
    public final boolean isAssignableFrom(Class<?> type) {
        return this.containerClass.isAssignableFrom(type);
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        return type instanceof ComplexType otherType && isAssignableFrom(otherType);
    }

    private boolean isAssignableFrom(ComplexType type) {
        if (!this.containerClass.isAssignableFrom(type.containerClass)) return false;
        if (subTypes.length != type.subTypes.length) return false;
        for (int i = 0; i < subTypes.length; i++) {
            if (!subTypes[i].isAssignableFrom(type.subTypes[i])) return false;
        }
        return true;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        return isAssignableFrom(value.getClass());
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ComplexType other = (ComplexType) obj;
        return isAssignableFrom(other) && other.isAssignableFrom(this);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), containerClass.hashCode(), Arrays.hashCode(subTypes));
    }
}
