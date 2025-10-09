package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.Flags;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.util.EqualUtil;
import io.axual.ksml.data.util.ValuePrinter;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static io.axual.ksml.data.object.DataObjectFlags.IGNORE_DATA_MAP_CONTENTS;
import static io.axual.ksml.data.object.DataObjectFlags.IGNORE_DATA_MAP_TYPE;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.objectNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;
import static io.axual.ksml.data.util.EqualUtil.typeNotEqual;

/**
 * Represents a map with {@code String} keys and {@link DataObject} values within the
 * {@link DataObject} framework.
 *
 * <p>The {@code DataMap} class provides a structured wrapper for handling collections
 * of {@link DataObject} elements. It builds on the functionality of Java's {@code Map}
 * while validating type metadata for each element, ensuring compatibility with the
 * type-driven {@code DataObject} framework.</p>
 *
 * @see DataObject
 * @see MapType
 */
@EqualsAndHashCode
public class DataMap implements DataObject {
    /**
     * Represents the static map type for maps of unknown element types.
     */
    private static final MapType MAP_OF_UNKNOWN = new MapType();

    /**
     * Represents the actual key-value pair data of the map, sorted alphabetically by key.
     * <p>
     * The {@link TreeMap} is chosen for its sorted nature, and the sorting is alphabetical.
     * </p>
     */
    private final TreeMap<String, DataObject> contents;

    /**
     * The value type information for this {@code DataMap} instance, represented by a {@link MapType}.
     */
    @JsonIgnore
    private final MapType type;

    /**
     * Constructs an empty {@code DataMap} with an unknown value type.
     */
    public DataMap() {
        this(null);
    }

    /**
     * Constructs an empty {@code DataMap} with the specified value type.
     *
     * <p>This constructor allows defining the type of elements the map should contain,
     * enabling type validation during data processing.</p>
     *
     * @param valueType The {@link DataType} of the elements to be stored in the map.
     */
    public DataMap(DataType valueType) {
        this(valueType, false);
    }

    /**
     * Constructs a {@code DataMap} with the specified value type and nullability flag.
     *
     * @param valueType The value type held this {@code DataMap}.
     * @param isNull    If {@code true}, the content is considered null.
     */
    public DataMap(DataType valueType, boolean isNull) {
        contents = !isNull ? new TreeMap<>() : null;
        type = valueType != null ? new MapType(valueType) : MAP_OF_UNKNOWN;
    }

    /**
     * Checks if the current map is null.
     *
     * @return {@code true} if the map content is null, otherwise {@code false}.
     */
    public boolean isNull() {
        return contents == null;
    }

    /**
     * Returns the type definition of this {@code DataMap}.
     *
     * @return The {@link MapType} of the map.
     */
    @Override
    public MapType type() {
        return type;
    }

    /**
     * Retrieves the value type of the elements in this {@code DataMap}.
     *
     * @return The {@link DataType} of the map's elements.
     */
    public DataType valueType() {
        return type.valueType();
    }

    /**
     * Verifies whether the given key exists in the content map.
     *
     * @param key The key to check for existence.
     * @return {@code true} if the key exists, otherwise {@code false}.
     */
    public boolean containsKey(String key) {
        return contents != null && contents.containsKey(key);
    }

    /**
     * Retrieves the entry set of this {@code DataMap}.
     * <p>If the data structure is null, an empty set is returned.</p>
     *
     * @return A set of {@link Map.Entry} objects representing the key-value pairs.
     */
    public Set<Map.Entry<String, DataObject>> entrySet() {
        return contents != null ? contents.entrySet() : Collections.emptySet();
    }

    /**
     * Performs the given action for each entry in this {@code DataMap}.
     * <p>If this map is null, the action is not executed.</p>
     *
     * @param action The action to perform for each key-value pair.
     */
    public void forEach(BiConsumer<String, ? super DataObject> action) {
        if (contents != null) contents.forEach(action);
    }

    /**
     * Retrieves the value for the specified key in this map.
     *
     * @param key The key for which the value is retrieved.
     * @return The value associated with the key, or {@code null} if the key is not found.
     */
    public DataObject get(String key) {
        return contents != null ? contents.get(key) : null;
    }

    /**
     * Verifies that the given {@link DataObject} matches the map's value type.
     *
     * @param value The {@link DataObject} to check.
     * @return The same {@link DataObject} if the type is valid.
     * @throws IllegalArgumentException if the value type is invalid.
     */
    private DataObject assignableValue(DataObject value) {
        final var assignable = type.valueType().isAssignableFrom(value.type());
        if (assignable.isError()) {
            throw new IllegalArgumentException("Can not cast value of dataType " + value.type() + " to " + type.valueType());
        }
        return value;
    }

    /**
     * Inserts the specified key-value pair into the {@code DataMap}.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     * @return The value.
     * @throws DataException If the map is null or the key-value pair cannot be added.
     */
    public DataObject put(String key, DataObject value) {
        if (contents == null)
            throw new DataException("Can not add item to a NULL Map: (" + (key != null ? key : "null") + ", " + (value != null ? value : "null") + ")");
        contents.put(key, assignableValue(value));
        return value;
    }

    /**
     * Inserts the specified key-value pair into the {@code DataMap} if the key does not exist already.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     * @return The value associated with the key.
     * @throws DataException If the map is null or the key-value pair cannot be added.
     */
    public DataObject putIfAbsent(String key, DataObject value) {
        if (contents == null)
            throw new DataException("Can not add item to a NULL Map: (" + (key != null ? key : "null") + ", " + (value != null ? value : "null") + ")");
        return contents.computeIfAbsent(key, k -> assignableValue(value));
    }

    /**
     * Inserts the specified key-value pair into the {@code DataMap} if value is not null.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     * @return The value associated with the key.
     */
    public DataObject putIfNotNull(String key, DataObject value) {
        return value != null ? put(key, value) : get(key);
    }

    /**
     * Retrieves the number of key-value pairs in this {@code DataMap}.
     *
     * @return The number of entries, or {@code 0} if the map is null.
     */
    public int size() {
        return contents != null ? contents.size() : 0;
    }


    /**
     * Retrieves a string representation of this {@code DataMap}.
     *
     * @return The string representation of this {@code DataMap}.
     */
    @Override
    public String toString() {
        return toString(Printer.INTERNAL);
    }

    /**
     * Retrieves a string representation of this {@code DataMap} using the given Printer.
     *
     * @return The string representation of this {@code DataMap}.
     */
    @Override
    public String toString(Printer printer) {
        final var schemaName = printer.schemaString(this);

        // Return NULL as value
        if (isNull()) return schemaName + "null";

        final var iterator = entrySet().iterator();

        // Return empty map as value
        if (!iterator.hasNext()) return schemaName + "{}";

        // Iterate through all key/value entries
        final var sb = new StringBuilder(schemaName).append("{");
        while (true) {
            final var entry = iterator.next();
            final var key = entry.getKey();
            final var value = entry.getValue();
            sb.append(ValuePrinter.print(key, true));
            sb.append(": ");
            sb.append(value != null ? value.toString(printer.childObjectPrinter()) : "null");
            if (!iterator.hasNext())
                return sb.append("}").toString();
            sb.append(", ");
        }
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equal equals(Object other, Flags flags) {
        if (this == other) return Equal.ok();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass()))
            return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (DataMap) other;

        // Compare type
        if (!flags.isSet(IGNORE_DATA_MAP_TYPE)) {
            final var typeEquals = type.equals(that.type, flags);
            if (typeEquals.isError())
                return typeNotEqual(type, that.type, typeEquals);
        }

        // Compare contents
        if (!flags.isSet(IGNORE_DATA_MAP_CONTENTS) && (contents != null || that.contents != null)) {
            if (contents == null || that.contents == null) return EqualUtil.objectNotEqual(this, that);
            final var contentsEqual = equalContents(this, that, flags);
            if (contentsEqual.isError()) return objectNotEqual(this, that, contentsEqual);
        }

        return Equal.ok();
    }

    private static Equal equalContents(DataMap left, DataMap right, Flags flags) {
        if (left.size() != right.size())
            return fieldNotEqual("contentSize", left, left.size(), right, right.size());

        for (var entry : left.entrySet()) {
            final var thatValue = right.get(entry.getKey());
            if (entry.getValue() != null || thatValue != null) {
                if (entry.getValue() == null || thatValue == null)
                    return fieldNotEqual(entry.getKey(), left, entry.getValue(), right, thatValue);
                final var entryEqual = entry.getValue().equals(thatValue, flags);
                if (entryEqual.isError())
                    return fieldNotEqual(entry.getKey(), left, entry.getValue(), right, thatValue, entryEqual);
            }
        }

        return Equal.ok();
    }
}
