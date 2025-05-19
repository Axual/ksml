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
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.util.ValuePrinter;
import lombok.Getter;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Represents a data structure that emulates a schema-based, key-value map with automatic sorting capabilities.
 * Designed to carry structured data with schema support, it provides utilities for navigating and manipulating
 * key-value pairs in a structured and type-safe manner.
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Automatic sorting of keys, prioritizing standard keys before "meta" keys (keys starting with {@value META_ATTRIBUTE_CHAR}).</li>
 *   <li>Schema-driven validation and type safety through {@link StructType} and {@link StructSchema}.</li>
 *   <li>Helper methods for safely retrieving and manipulating key-value entries.</li>
 *   <li>Allows nullability for struct representation, catering to scenarios where absence of content needs clear handling.</li>
 * </ul>
 *
 * <p>This implementation ensures consistent ordering of keys and maintains safety through its encapsulated operations.</p>
 */
@Getter
public class DataStruct implements DataObject {
    /**
     * Character used for identifying "meta" keys. Meta keys (keys that start with this character)
     * have a lower priority compared to standard keys when sorted.
     */
    public static final String META_ATTRIBUTE_CHAR = "@";

    /**
     * Custom comparator for sorting key-value pairs in the data structure.
     * <p>
     * The sorting logic prioritizes standard keys over "meta" keys, and within each group,
     * sorts lexicographically.
     */
    public static final Comparator<String> COMPARATOR = (o1, o2) -> {
        if ((o1 == null || o1.isEmpty()) && (o2 == null || o2.isEmpty())) return 0;
        if (o1 == null || o1.isEmpty()) return -1;
        if (o2 == null || o2.isEmpty()) return 1;

        final var meta1 = o1.startsWith(META_ATTRIBUTE_CHAR);
        final var meta2 = o2.startsWith(META_ATTRIBUTE_CHAR);

        // If only the first string starts with the meta char, then sort it last
        if (meta1 && !meta2) return 1;
        // If only the second string starts with the meta char, then sort it first
        if (!meta1 && meta2) return -1;
        // If both (do not) start with the meta char, then sort as normal
        return o1.compareTo(o2);
    };

    /**
     * Represents the actual key-value pair data of the struct.
     * <p>
     * The {@link TreeMap} is chosen for its sorted nature, and the sorting behavior is determined
     * by the {@link #COMPARATOR}.
     * </p>
     */
    private final TreeMap<String, DataObject> contents;

    /**
     * The type of the struct, represented as a {@link StructType}.
     * <p>
     * Encapsulates schema details for the data struct, associating its content
     * with the specific schema structure defined by {@link StructSchema}.
     * </p>
     */
    @JsonIgnore
    private final StructType type;

    /**
     * Functional interface for applying custom operations on certain struct values of a specific type.
     *
     * @param <T> The type of the value to be applied.
     */
    public interface DataStructApplier<T> {
        /**
         * Applies the operation to the specified value.
         *
         * @param value the value to be processed.
         * @throws Exception if the operation fails.
         */
        void apply(T value) throws Exception;
    }

    /**
     * Constructs a {@code DataStruct} with no schema and an empty content map.
     */
    public DataStruct() {
        this(null);
    }

    /**
     * Constructs a {@code DataStruct} with the specified schema.
     *
     * @param schema The schema defining the structure of this {@code DataStruct}.
     */
    public DataStruct(StructSchema schema) {
        this(schema, false);
    }

    /**
     * Constructs a {@code DataStruct} with the specified schema and nullability flag.
     *
     * @param schema The schema defining the structure of this {@code DataStruct}.
     * @param isNull If {@code true}, the content is considered null.
     */
    public DataStruct(StructSchema schema, boolean isNull) {
        contents = !isNull ? new TreeMap<>(COMPARATOR) : null;
        type = new StructType(schema);
    }

    /**
     * Checks if the current struct is null.
     *
     * @return {@code true} if the struct content is null, otherwise {@code false}.
     */
    public boolean isNull() {
        return contents == null;
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
     * Retrieves the entry set of this {@code DataStruct}.
     * <p>If the data structure is null, an empty set is returned.</p>
     *
     * @return A set of {@link Map.Entry} objects representing the key-value pairs.
     */
    public Set<Map.Entry<String, DataObject>> entrySet() {
        return contents != null ? contents.entrySet() : Collections.emptySet();
    }

    /**
     * Performs the given action for each entry in this {@code DataStruct}.
     * <p>If this struct is null, the action is not executed.</p>
     *
     * @param action The action to perform for each key-value pair.
     */
    public void forEach(BiConsumer<String, DataObject> action) {
        if (contents != null) contents.forEach(action);
    }

    /**
     * Retrieves the value for the specified key in this struct.
     *
     * @param key The key for which the value is retrieved.
     * @return The value associated with the key, or {@code null} if the key is not found.
     */
    public DataObject get(String key) {
        return contents != null ? contents.get(key) : null;
    }

    /**
     * Retrieves a value of the specified type for the given key and applies the provided operation.
     *
     * @param <T>     The type of the value.
     * @param key     The key for which the value is to be retrieved.
     * @param clazz   The expected type of the value.
     * @param applier The operation to apply on the value.
     * @throws DataException If the operation fails.
     */
    @SuppressWarnings("unchecked")
    public <T> void getIfPresent(String key, Class<T> clazz, DataStructApplier<T> applier) {
        final var value = get(key);
        if (value != null && clazz.isAssignableFrom(value.getClass())) {
            try {
                applier.apply((T) value);
            } catch (Exception e) {
                throw new DataException("Exception thrown while getting and applying DataStruct value for key \"" + key + "\"", e);
            }
        }
    }

    /**
     * Retrieves a value of a given key and return that if it matches the specified type, null otherwise.
     *
     * @param <T>   The type of the value.
     * @param key   The key for which the value is to be retrieved.
     * @param clazz The expected type of the value.
     */
    public <T> T getAs(String key, Class<T> clazz) {
        return getAs(key, clazz, null);
    }

    /**
     * Retrieves a value of a given key and return that if it matches the specified type, defaultValue otherwise.
     *
     * @param <T>          The type of the value.
     * @param key          The key for which the value is to be retrieved.
     * @param clazz        The expected type of the value.
     * @param defaultValue The defaultValue to return if they key is not found, or the expected type does not match.
     */
    public <T> T getAs(String key, Class<T> clazz, T defaultValue) {
        final var value = get(key);
        if (value != null && clazz.isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        return defaultValue;
    }

    /**
     * Retrieves a value of a given key and return that as String.
     *
     * @param key The key for which the value is to be retrieved.
     * @return The value associated with the key, returned as string value.
     */
    public DataString getAsString(String key) {
        final var result = get(key);
        return result instanceof DataString str ? str : result != null ? new DataString(result.toString()) : null;
    }

    /**
     * Inserts the specified key-value pair into the {@code DataStruct}.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     * @throws DataException If the struct is null or the key-value pair cannot be added.
     */
    public void put(String key, DataObject value) {
        if (contents == null)
            throw new DataException("Can not add item to a NULL Struct: (" + (key != null ? key : "null") + ", " + (value != null ? value : "null") + ")");
        contents.put(key, value);
    }

    /**
     * Inserts the specified key-value pair into the {@code DataStruct} if the key does not exist already.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     * @throws DataException If the struct is null or the key-value pair cannot be added.
     */
    public void putIfAbsent(String key, DataObject value) {
        if (contents == null)
            throw new DataException("Can not add item to a NULL Struct: (" + (key != null ? key : "null") + ", " + (value != null ? value : "null") + ")");
        contents.putIfAbsent(key, value);
    }

    /**
     * Inserts the specified key-value pair into the {@code DataStruct} if value is not null.
     *
     * @param key   The key to insert.
     * @param value The value to associate with the key.
     */
    public void putIfNotNull(String key, DataObject value) {
        if (value != null) put(key, value);
    }

    /**
     * Retrieves the number of key-value pairs in this {@code DataStruct}.
     *
     * @return The number of entries, or {@code 0} if the struct is null.
     */
    public int size() {
        return contents != null ? contents.size() : 0;
    }


    /**
     * Retrieves a string representation of this {@code DataStruct}.
     *
     * @return The string representation of this {@code DataStruct}.
     */
    @Override
    public String toString() {
        return toString(Printer.INTERNAL);
    }

    /**
     * Retrieves a string representation of this {@code DataStruct} using the given Printer.
     *
     * @return The string representation of this {@code DataStruct}.
     */
    @Override
    public String toString(Printer printer) {
        final var schemaName = printer.schemaString(this);

        // Return NULL as value
        if (isNull()) return schemaName + "null";

        final var iterator = entrySet().iterator();

        // Return empty struct as value
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
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        DataStruct that = (DataStruct) other;
        if (!type.isAssignableFrom(that.type) || !that.type.isAssignableFrom(type)) return false;
        if (contents.size() != that.contents.size()) return false;
        for (final var content : contents.entrySet()) {
            final var thisContent = content.getValue();
            final var thatContent = that.contents.get(content.getKey());
            if (!thisContent.equals(thatContent))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), contents, type);
    }
}
