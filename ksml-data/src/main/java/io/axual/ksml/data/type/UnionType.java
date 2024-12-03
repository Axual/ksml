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

import static io.axual.ksml.data.schema.DataField.NO_INDEX;

@Getter
public class UnionType extends ComplexType {
    private static final String UNION_NAME = "Union";
    private final ValueType[] valueTypes;

    // A field type
    public record ValueType(String name, DataType type, int index) {
        public ValueType(DataType type) {
            this(null, type, NO_INDEX);
        }
    }

    public UnionType(ValueType... valueTypes) {
        super(Object.class, valueTypesToDataTypes(valueTypes));
        this.valueTypes = valueTypes;
    }

    private static DataType[] valueTypesToDataTypes(ValueType... valueTypes) {
        var result = new DataType[valueTypes.length];
        for (int index = 0; index < valueTypes.length; index++) {
            result[index] = valueTypes[index].type();
        }
        return result;
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
        if (type instanceof UnionType otherUnion && isAssignableFromOtherUnion(otherUnion)) return true;

        // If the union did not match in its entirety, then check for assignable subtypes
        for (var valueType : valueTypes) {
            if (valueType.type.isAssignableFrom(type)) return true;
        }
        return false;
    }

    private boolean isAssignableFromOtherUnion(UnionType other) {
        var otherValueTypes = other.valueTypes();
        if (valueTypes.length != otherValueTypes.length) return false;
        for (int index = 0; index < valueTypes.length; index++) {
            if (!valueTypes[index].type.isAssignableFrom(otherValueTypes[index]))
                return false;
            if (!otherValueTypes[index].type.isAssignableFrom(valueTypes[index]))
                return false;
        }
        return true;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        for (final var valueType : valueTypes) {
            if (valueType.type.isAssignableFrom(value)) return true;
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
        return Objects.hash(super.hashCode(), Arrays.hashCode(valueTypes));
    }
}
