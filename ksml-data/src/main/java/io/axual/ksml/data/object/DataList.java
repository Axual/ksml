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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Represents a list of {@link DataObject} instances within the {@link DataObject} framework.
 *
 * <p>The {@code DataList} class provides a structured wrapper for handling collections
 * of {@link DataObject} elements. It extends the functionality of Java's {@code ArrayList}
 * while maintaining type metadata for each element, ensuring compatibility with the
 * schema-driven {@code DataObject} framework.</p>
 *
 * @see DataObject
 * @see ListType
 */
public class DataList implements DataObject, Iterable<DataObject> {
    /**
     * Represents the static list type for lists of unknown element types.
     */
    private static final ListType LIST_OF_UNKNOWN = new ListType(DataType.UNKNOWN);

    /**
     * The internal list of {@link DataObject} instances stored in this {@code DataList}.
     */
    private final ArrayList<DataObject> contents;

    /**
     * The schema type information for this {@code DataList} instance, represented by a {@link ListType}.
     */
    @JsonIgnore
    private final ListType type;

    /**
     * Constructs an empty {@code DataList} with an unknown value type.
     */
    public DataList() {
        this(DataType.UNKNOWN);
    }

    /**
     * Constructs an empty {@code DataList} with the specified value type.
     *
     * <p>This constructor allows defining the type of elements the list should contain,
     * enabling schema validation during data processing.</p>
     *
     * @param valueType The {@link DataType} of the elements to be stored in the list.
     */
    public DataList(DataType valueType) {
        this(valueType, false);
    }

    /**
     * Constructs a {@code DataList} with the specified value type and nullability.
     *
     * <p>If {@code isNull} is {@code true}, the list will be initialized as null
     * rather than as an empty list.</p>
     *
     * @param valueType The {@link DataType} of the elements to be stored in the list.
     * @param isNull    Whether this {@code DataList} should be initialized as null.
     */
    public DataList(DataType valueType, boolean isNull) {
        contents = !isNull ? new ArrayList<>() : null;
        type = valueType != null ? new ListType(valueType) : LIST_OF_UNKNOWN;
    }

    /**
     * Checks if this {@code DataList} has no content (i.e., is null).
     *
     * @return {@code true} if the list is null, {@code false} otherwise.
     */
    public boolean isNull() {
        return contents == null;
    }

    /**
     * Adds a {@link DataObject} value to the list if it is not null.
     *
     * <p>If the value is null, it will not be added to the list.</p>
     *
     * @param value The {@link DataObject} to add to the list.
     */
    public void addIfNotNull(DataObject value) {
        if (value != null) add(value);
    }

    /**
     * Returns the type definition of this {@code DataList}.
     *
     * @return The {@link ListType} of the list.
     */
    @Override
    public ListType type() {
        return type;
    }

    /**
     * Retrieves the value type of the elements in this {@code DataList}.
     *
     * @return The {@link DataType} of the list's elements.
     */
    public DataType valueType() {
        return type.valueType();
    }

    /**
     * Verifies that the given {@link DataObject} matches the list's value type.
     *
     * @param value The {@link DataObject} to check.
     * @return The same {@link DataObject} if the type is valid.
     * @throws IllegalArgumentException if the value type is invalid.
     */
    private DataObject verifiedValue(DataObject value) {
        if (!type.valueType().isAssignableFrom(value.type())) {
            throw new IllegalArgumentException("Can not cast value of dataType " + value.type() + " to " + type.valueType());
        }
        return value;
    }

    /**
     * Adds an element to the list after verifying its type against the list's value type.
     *
     * @param value The {@link DataObject} to add.
     * @return {@code true} if the element was successfully added, {@code false} otherwise.
     * @throws IllegalArgumentException if the value type is invalid.
     */
    public boolean add(DataObject value) {
        return contents.add(verifiedValue(value));
    }

    /**
     * Returns an iterator over the elements in this list.
     *
     * @return An {@link Iterator} over {@link DataObject} elements.
     */
    @Nonnull
    @Override
    public Iterator<DataObject> iterator() {
        if (contents == null) return Collections.emptyIterator();
        return contents.iterator();
    }

    /**
     * Returns the size of the list, indicating the number of elements it contains.
     *
     * @return The size of the list.
     */
    public int size() {
        return contents != null ? contents.size() : 0;
    }

    /**
     * Retrieves the element at the specified index in the list.
     *
     * @param index The index of the element to retrieve.
     * @return The {@link DataObject} at the specified index.
     * @throws IndexOutOfBoundsException if the index is invalid.
     */
    public DataObject get(int index) {
        return contents.get(index);
    }

    /**
     * Checks if the list is empty.
     *
     * @return {@code true} if the list contains no elements, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return contents.isEmpty();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DataList otherList)) return false;
        if (!type.isAssignableFrom(otherList.type)) return false;
        if (contents.size() != otherList.contents.size()) return false;
        for (int index = 0; index < contents.size(); index++) {
            final var element = contents.get(index);
            final var otherElement = otherList.contents.get(index);
            if (!element.equals(otherElement)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type.hashCode(), contents);
    }

    @Override
    public String toString() {
        return toString(Printer.INTERNAL);
    }

    /**
     * Converts the list to its string representation using the specified {@link Printer}.
     *
     * @param printer The {@link Printer} used to format the string output.
     * @return A string representation of the list.
     */
    @Override
    public String toString(Printer printer) {
        final var sb = new StringBuilder(printer.schemaString(this));
        sb.append("[");
        for (int index = 0; index < size(); index++) {
            if (index > 0) sb.append(", ");
            sb.append(get(index).toString(printer.childObjectPrinter()));
        }
        sb.append("]");
        return sb.toString();
    }
}
