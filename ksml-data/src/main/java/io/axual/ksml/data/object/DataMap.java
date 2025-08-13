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
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.util.ValuePrinter;

import java.util.*;
import java.util.function.BiConsumer;

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
    private DataObject verifiedValue(DataObject value) {
        if (!type.valueType().isAssignableFrom(value.type())) {
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
        contents.put(key, verifiedValue(value));
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
        return contents.computeIfAbsent(key, k -> verifiedValue(value));
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        final DataMap dataMap = (DataMap) other;
        return Objects.equals(type, dataMap.type) && Objects.equals(contents, dataMap.contents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), contents, type);
    }
}
