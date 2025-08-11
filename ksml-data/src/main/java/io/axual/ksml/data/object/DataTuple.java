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

import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.value.Tuple;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;

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
}
