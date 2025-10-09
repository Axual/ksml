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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.util.AssignableUtil;
import io.axual.ksml.data.util.EqualUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.axual.ksml.data.type.DataTypeFlags.IGNORE_DATA_TYPE_CONTAINER_CLASS;
import static io.axual.ksml.data.util.AssignableUtil.fieldNotAssignable;
import static io.axual.ksml.data.util.AssignableUtil.typeMismatch;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * Base class for composite {@link DataType} implementations that have one or more subtypes.
 * <p>
 * Examples include lists, maps, tuples, unions and structs. ComplexType provides helpers to
 * build readable names/specs and implements assignability rules that compare the container
 * class and all subtypes.
 */
@EqualsAndHashCode(exclude = {"name", "spec"})
@Getter
public abstract class ComplexType implements DataType {
    private final Class<?> containerClass;
    private final DataType[] subTypes;
    private final String name;
    private final String spec;

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
    public Assignable isAssignableFrom(DataType type) {
        if (!(type instanceof ComplexType that))
            return typeMismatch(this, type);

        // Check containerClass
        if (!containerClass.isAssignableFrom(that.containerClass))
            return fieldNotAssignable("containerClass", this, containerClass, that, that.containerClass);

        // Check subTypes
        if (subTypes.length != that.subTypes.length)
            return AssignableUtil.fieldNotAssignable("subTypeCount", this, subTypes.length, that, that.subTypes.length);
        for (int i = 0; i < subTypes.length; i++) {
            final var subTypeAssignable = subTypes[i].isAssignableFrom(that.subTypes[i]);
            if (subTypeAssignable.isError())
                return AssignableUtil.fieldNotAssignable("subTypes[" + i + "]", this, subTypes[i], that, that.subTypes[i], subTypeAssignable);
        }

        return Assignable.ok();
    }

    @Override
    public Equal equals(Object obj, Flags flags) {
        if (this == obj) return Equal.ok();
        if (obj == null) return otherIsNull(this);
        if (!getClass().equals(obj.getClass())) return EqualUtil.containerClassNotEqual(getClass(), obj.getClass());
        final var that = (ComplexType) obj;
        if (!flags.isSet(IGNORE_DATA_TYPE_CONTAINER_CLASS) && !containerClass.equals(that.containerClass))
            return EqualUtil.containerClassNotEqual(containerClass, that.containerClass);
        return subTypesEqual((ComplexType) obj, flags);
    }

    private Equal subTypesEqual(ComplexType other, Flags flags) {
        if (subTypes.length != other.subTypes.length)
            return Equal.error("Type \"" + this + "\" has a different number of subtypes than \"" + other + "\"");
        for (int i = 0; i < subTypes.length; i++) {
            final var subTypeEqual = subTypes[i].equals(other.subTypes[i], flags);
            if (subTypeEqual.isError()) return subTypeEqual;
        }
        return Equal.ok();
    }

    @Override
    public String toString() {
        return name;
    }
}
