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

import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

/**
 * Base class for composite {@link DataType} implementations that have one or more sub-types.
 * <p>
 * Examples include lists, maps, tuples, unions and structs. ComplexType provides helpers to
 * build readable names/specs and implements assignability rules that compare the container
 * class and all sub-types.
 */
@Getter
public abstract class ComplexType implements DataType {
    private final Class<?> containerClass;
    private final String name;
    private final String spec;
    private final DataType[] subTypes;

    protected ComplexType(Class<?> containerClass, String name, String spec, DataType... subTypes) {
        this.containerClass = containerClass;
        this.name = name;
        this.spec = spec;
        this.subTypes = subTypes;
    }

    protected static String buildName(String baseName, DataType... subTypes) {
        return buildName(baseName, "Of", subTypes);
    }

    protected static String buildName(String baseName, String midString, DataType... subTypes) {
        return buildName(baseName, midString, "And", subTypes);
    }

    protected static String buildName(String baseName, String midString, String subTypeConcatenation, DataType... subTypes) {
        StringBuilder builder = new StringBuilder(baseName);
        if (subTypes.length > 0) {
            builder.append(midString);
            for (int index = 0; index < subTypes.length; index++) {
                if (index > 0) builder.append(subTypeConcatenation);
                final var subTypeName = subTypes[index].name();
                if (subTypeName != null && !subTypeName.isEmpty()) {
                    builder.append(subTypeName.substring(0, 1).toUpperCase()).append(subTypeName.substring(1));
                }
            }
        }
        return builder.toString();
    }

    protected static String buildSpec(DataType... subTypes) {
        StringBuilder builder = new StringBuilder();
        if (subTypes.length > 0) {
            for (int index = 0; index < subTypes.length; index++) {
                if (index > 0) builder.append(", ");
                builder.append(subTypes[index].spec());
            }
        }
        return builder.toString();
    }

    public int subTypeCount() {
        return subTypes.length;
    }

    public DataType subType(int index) {
        return subTypes[index];
    }

    @Override
    public final ValidationResult checkAssignableFrom(Class<?> otherContainerClass, ValidationContext context) {
        if (!containerClass.isAssignableFrom(otherContainerClass)) {
            return context.typeMismatch(this, otherContainerClass);
        }
        return context;
    }

    @Override
    public ValidationResult checkAssignableFrom(DataType otherType, ValidationContext context) {
        if (otherType instanceof ComplexType otherComplexType) {
            if (!checkAssignableFrom(otherComplexType.containerClass, context).isOK()) {
                return context.typeMismatch(this, otherComplexType.containerClass);
            }
            if (subTypes.length != otherComplexType.subTypes.length) {
                return context.addError("Type \"" + context.thatType(otherType) + "\" has a different number of sub-types than \"" + context.thisType(this) + "\"");
            } else {
                for (int i = 0; i < subTypes.length; i++) {
                    subTypes[i].checkAssignableFrom(otherComplexType.subTypes[i], context);
                }
                return context;
            }
        } else if (otherType != null) {
            return context.addError("Can not assign \"" + otherType + "\" to \"" + this + "\".");
        } else {
            return context.addError("No type specified, this is a bug in KSML");
        }
    }

    @Override
    public String toString() {
        return name;
    }

    private boolean subTypeEquals(ComplexType other) {
        if (subTypes.length != other.subTypes.length) return false;
        for (int i = 0; i < subTypes.length; i++) {
            if (!subTypes[i].equals(other.subTypes[i])) return false;
        }
        return true;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        return obj != null
                && getClass().equals(obj.getClass())
                && containerClass.equals(((ComplexType) obj).containerClass)
                && subTypeEquals((ComplexType) obj);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), containerClass.hashCode(), Arrays.hashCode(subTypes));
    }
}
