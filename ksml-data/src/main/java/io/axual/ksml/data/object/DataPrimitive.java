package io.axual.ksml.data.object;

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

import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.util.EqualUtil;
import io.axual.ksml.data.util.JavaValuePrinter;
import io.axual.ksml.data.util.ValuePrinter;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.axual.ksml.data.object.DataObjectFlag.IGNORE_DATA_PRIMITIVE_TYPE;
import static io.axual.ksml.data.object.DataObjectFlag.IGNORE_DATA_PRIMITIVE_VALUE;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * Represents a wrapper for a primitive value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataPrimitive} class encapsulates a primitive value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables primitive values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
@EqualsAndHashCode
@Getter
public class DataPrimitive<T> implements DataObject {
    private static final ValuePrinter VALUE_PRINTER = new JavaValuePrinter();
    private final DataType type;
    private final T value;

    protected DataPrimitive(DataType type, T value) {
        this.type = type;
        this.value = value;
        checkValue();
    }

    private void checkValue() {
        final var assignable = value instanceof DataObject dataObject
                ? type.isAssignableFrom(dataObject)
                : type.isAssignableFrom(value);
        if (assignable.isNotAssignable())
            throw new DataException("Value assigned to " + type + " can not be \"" + this + "\": " + assignable);
    }

    /**
     * Retrieves a string representation of this {@code DataPrimitive}.
     *
     * @return The string representation of this {@code DataPrimitive}.
     */
    @Override
    public String toString() {
        return toString(Printer.INTERNAL);
    }

    /**
     * Retrieves a string representation of this {@code DataPrimitive} using the given Printer.
     *
     * @return The string representation of this {@code DataPrimitive}.
     */
    @Override
    public String toString(Printer printer) {
        return value != null
                ? VALUE_PRINTER.print(value, printer != Printer.INTERNAL)
                : printer.forceSchemaPrefix(this) + VALUE_PRINTER.print(null, printer != Printer.INTERNAL);
    }

    @Override
    public Equality equals(Object other, EqualityFlags flags) {
        if (this == other) return Equality.equal();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass())) return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (DataPrimitive<?>) other;

        final var typeEquality = compareType(that, flags);
        if (typeEquality != null) return typeEquality;

        final var valueEquality = compareValue(that, flags);
        if (valueEquality != null) return valueEquality;

        return Equality.equal();
    }

    private Equality compareType(DataPrimitive<?> that, EqualityFlags flags) {
        if (flags.isSet(IGNORE_DATA_PRIMITIVE_TYPE)) return null;
        final var typeEqual = type.equals(that.type, flags);
        if (typeEqual.isNotEqual()) return fieldNotEqual("type", this, type, that, that.type, typeEqual);
        return null;
    }

    private Equality compareValue(DataPrimitive<?> that, EqualityFlags flags) {
        if (flags.isSet(IGNORE_DATA_PRIMITIVE_VALUE)) return null;
        // Exactly one of the two values is null: not equal. Both null: equal, nothing more to compare.
        if ((value == null) != (that.value == null)) return EqualUtil.objectNotEqual(this, that);
        if (value == null) return null;

        if (value instanceof DataObject dataValue) {
            final var valueEqual = dataValue.equals(that.value, flags);
            if (valueEqual.isNotEqual())
                return fieldNotEqual("value", this, dataValue, that, that.value, valueEqual);
            return null;
        }
        if (!value.equals(that.value)) return fieldNotEqual("value", this, value, that, that.value);
        return null;
    }
}
