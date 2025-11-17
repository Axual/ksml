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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.util.EqualUtil;
import io.axual.ksml.data.value.Tuple;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;

import static io.axual.ksml.data.object.DataObjectFlag.IGNORE_DATA_TUPLE_CONTENTS;
import static io.axual.ksml.data.object.DataObjectFlag.IGNORE_DATA_TUPLE_TYPE;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.objectNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;
import static io.axual.ksml.data.util.EqualUtil.typeNotEqual;

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
        final var sb = new StringBuilder(printer.schemaPrefix(this));
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
    public Equality equals(Object other, EqualityFlags flags) {
        if (this == other) return Equality.equal();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass())) return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (DataTuple) other;

        // Compare type
        if (!flags.isSet(IGNORE_DATA_TUPLE_TYPE)) {
            final var typeEqual = type.equals(that.type, flags);
            if (typeEqual.isNotEqual())
                return typeNotEqual(type, that.type, typeEqual);
        }

        // Compare contents
        if (!flags.isSet(IGNORE_DATA_TUPLE_CONTENTS)) {
            if (elements() != null || that.elements() != null) {
                if (elements() == null || that.elements() == null) return EqualUtil.objectNotEqual(this, that);
                final var contentsEqual = equalContents(this, that, flags);
                if (contentsEqual.isNotEqual()) return objectNotEqual(this, that, contentsEqual);
            }
        }

        return Equality.equal();
    }

    private static Equality equalContents(DataTuple left, DataTuple right, EqualityFlags flags) {
        if (left.elements().size() != right.elements().size())
            return fieldNotEqual("elementCount", left, left.elements().size(), right, right.elements().size());

        var index = 0;
        for (var element : left.elements()) {
            final var thatElement = right.elements().get(index);
            if (element != null || thatElement != null) {
                if (element == null || thatElement == null)
                    return fieldNotEqual("field[" + index + "]", left, element, right, thatElement);
                final var entryEqual = element.equals(thatElement, flags);
                if (entryEqual.isNotEqual())
                    return fieldNotEqual("field[" + index + "]", left, element, right, thatElement, entryEqual);
            }
            index++;
        }
        return Equality.equal();
    }
}
