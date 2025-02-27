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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.util.ValuePrinter;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

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
@Getter
public class DataPrimitive<T> implements DataObject {
    private final DataType type;
    private final T value;

    protected DataPrimitive(DataType type, T value) {
        this.type = type;
        this.value = value;
        checkValue();
    }

    private void checkValue() {
        final var valid = value instanceof DataObject dataObject
                ? type.isAssignableFrom(dataObject)
                : type.isAssignableFrom(value);
        if (!valid) throw new DataException("Value assigned to " + type + " can not be \"" + this + "\"");
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
                ? ValuePrinter.print(value, printer != Printer.INTERNAL)
                : printer.forceSchemaString(this) + ValuePrinter.print(null, printer != Printer.INTERNAL);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        DataPrimitive<?> that = (DataPrimitive<?>) other;
        if (value == null) return that.value == null;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
