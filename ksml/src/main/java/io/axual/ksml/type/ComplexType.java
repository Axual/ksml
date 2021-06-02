package io.axual.ksml.type;

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


import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ComplexType implements DataType {
    public final Class<?> type;
    public final DataType[] subTypes;

    @Override
    public String toString() {
        var subTypeStr = new StringBuilder();
        for (DataType subType : subTypes) {
            subTypeStr.append(subTypeStr.length() > 0 ? ", " : "").append(subType);
        }
        return type.getSimpleName() + "<" + subTypeStr + ">";
    }

    @Override
    public boolean isAssignableFrom(Class<?> type) {
        return this.type.isAssignableFrom(type);
    }

    @Override
    public boolean isAssignableFrom(DataType type) {
        return type instanceof ComplexType && isAssignableFrom((ComplexType) type);
    }

    private boolean isAssignableFrom(ComplexType type) {
        if (!this.type.isAssignableFrom(type.type)) return false;
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
        int result = super.hashCode();
        result = result * 31 + type.hashCode();
        for (DataType subType : subTypes) {
            result = result * 31 + subType.hashCode();
        }
        return result;
    }
}
