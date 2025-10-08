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

import io.axual.ksml.data.compare.Compared;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.Flags;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.value.Tuple;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

import static io.axual.ksml.data.type.EqualityFlags.IGNORE_DATA_TUPLE_CONTENTS;
import static io.axual.ksml.data.type.EqualityFlags.IGNORE_DATA_TUPLE_TYPE;

/**
 * Represents a tuple of {@link DataObject} values within the {@link DataObject} framework.
 *
 * <p>The {@code DataTuple} class wraps an ordered, fixed-size collection of {@link DataObject}
 * elements and provides a {@link io.axual.ksml.data.type.TupleType} describing the element types.
 * This allows tuples to participate in the schema-aware KSML data model and be rendered/validated
 * consistently with other data objects.</p>
 *
 * @see DataObject
 */
@Getter
@EqualsAndHashCode
public class DataTuple extends Tuple<DataObject> implements DataObject {
    /**
     * The tuple's {@link DataType} metadata describing the types of its elements
     * (implemented as a {@link TupleType}).
     */
    private final DataType type;

    /**
     * Constructs a {@code DataTuple} from the provided {@link DataObject} elements.
     *
     * <p>The element types are collected to form the {@link TupleType} for this tuple.</p>
     *
     * @param elements The ordered elements of this tuple.
     */
    public DataTuple(DataObject... elements) {
        super(elements);
        this.type = new TupleType(elementsToDataTypes(elements));
    }

    /**
     * Converts an array of {@link DataObject} elements to their corresponding {@link DataType}s.
     */
    private static DataType[] elementsToDataTypes(DataObject... elements) {
        return Arrays.stream(elements).map(DataObject::type).toArray(DataType[]::new);
    }

    /**
     * Retrieves a string representation of this {@code DataTuple}.
     *
     * @return The string representation of this {@code DataTuple}.
     */
    @Override
    public String toString() {
        return toString(Printer.INTERNAL);
    }

    /**
     * Retrieves a string representation of this {@code DataTuple} using the given Printer.
     *
     * @return The string representation of this {@code DataTuple}.
     */
    @Override
    public String toString(Printer printer) {
        final var sb = new StringBuilder(printer.schemaString(this));
        sb.append("(");
        for (int index = 0; index < elements().size(); index++) {
            if (index > 0) sb.append(", ");
            sb.append(elements().get(index).toString(printer.childObjectPrinter()));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Compared equals(Object other, Flags flags) {
        if (this == other) return Compared.ok();
        if (other == null) return Compared.otherIsNull(this);
        if (!getClass().equals(other.getClass())) return Compared.notEqual(getClass(), other.getClass());

        final var that = (DataTuple) other;

        // Compare type
        if (!flags.isSet(IGNORE_DATA_TUPLE_TYPE)) {
            final var typeCompared = type.equals(that.type, flags);
            if (typeCompared.isError())
                return Compared.notEqual(type, that.type, typeCompared);
        }

        // Compare contents
        if (!flags.isSet(IGNORE_DATA_TUPLE_CONTENTS)) {
            if (elements() == null) return that.elements() != null ? Compared.notEqual(this, that) : Compared.ok();
            if (that.elements() == null) return Compared.notEqual(this, that);
            if (elements().size() != that.elements().size()) return Compared.notEqual(this, that);
            final var contentsCompared = contentsEqual(elements(), that.elements(), flags);
            if (contentsCompared.isError()) return Compared.notEqual(this, that, contentsCompared);
        }

        return Compared.ok();
    }

    private static Compared contentsEqual(List<DataObject> left, List<DataObject> right, Flags flags) {
        var index = 0;
        for (var entry : left) {
            final var thatValue = right.get(index);
            if (entry == null && thatValue == null) continue;
            if (entry == null || thatValue == null) return Compared.notEqual(entry, thatValue);
            final var entryCompared = entry.equals(thatValue, flags);
            if (entryCompared.isError())
                return Compared.fieldNotEqual("[" + index + "]", "DataTuple", entry, thatValue, entryCompared);
            index++;
        }
        return Compared.ok();
    }
}
