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
 * Represents a wrapper for a boolean value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataBoolean} class encapsulates a boolean value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables boolean values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
@Getter
@EqualsAndHashCode
public class DataTuple extends Tuple<DataObject> implements DataObject {
    /**
     * Represents the data type of this {@code DataBoolean}, which is {@code Boolean},
     * mapped to the schema definition in {@link DataSchemaConstants#BOOLEAN_TYPE}.
     * <p>This constant ensures that the type metadata for a {@code DataBoolean} is
     * consistent across all usages in the framework.</p>
     */
    private final DataType type;

    /**
     * Constructs an empty {@code DataList} with the specified value type.
     *
     * <p>This constructor allows defining the type of elements the list should contain,
     * enabling schema validation during data processing.</p>
     *
     * @param elements The {@link DataType} of the elements to be stored in the list.
     */
    public DataTuple(DataObject... elements) {
        super(elements);
        this.type = new TupleType(elementsToDataTypes(elements));
    }

    /**
     * Converts an array of DataObjects
     *
     * @return The string representation of this {@code DataTuple}.
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
        return printer.schemaString(this) + super.toString();
    }
}
