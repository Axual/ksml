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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.*;
import java.util.function.BiConsumer;

@Getter
@EqualsAndHashCode
public class DataStruct implements DataObject {
    public static final String META_ATTRIBUTE_CHAR = "@";
    // To make external representations look nice, we base Structs on sorted maps. Sorting is done
    // based on keys, where "normal" keys are always sorted before "meta" keys.
    public static final Comparator<String> COMPARATOR = (o1, o2) -> {
        if ((o1 == null || o1.isEmpty()) && (o2 == null || o2.isEmpty())) return 0;
        if (o1 == null || o1.isEmpty()) return -1;
        if (o2 == null || o2.isEmpty()) return 1;

        var meta1 = o1.startsWith(META_ATTRIBUTE_CHAR);
        var meta2 = o2.startsWith(META_ATTRIBUTE_CHAR);

        // If only the first string starts with the meta char, then sort it last
        if (meta1 && !meta2) return 1;
        // If only the second string starts with the meta char, then sort it first
        if (!meta1 && meta2) return -1;
        // If both (do not) start with the meta char, then sort as normal
        return o1.compareTo(o2);
    };
    private static final String QUOTE = "\"";

    public interface DataStructApplier<T> {
        void apply(T value) throws Exception;
    }

    private final TreeMap<String, DataObject> contents;
    private final transient StructType type;

    public DataStruct() {
        this(null);
    }

    public DataStruct(StructSchema schema) {
        this(schema, false);
    }

    public DataStruct(StructSchema schema, boolean isNull) {
        contents = !isNull ? new TreeMap<>(COMPARATOR) : null;
        type = new StructType(schema);
    }

    public boolean isNull() {
        return contents == null;
    }

    public boolean containsKey(String key) {
        return contents != null && contents.containsKey(key);
    }

    public Set<Map.Entry<String, DataObject>> entrySet() {
        return contents != null ? contents.entrySet() : Collections.EMPTY_SET;
    }

    public void forEach(BiConsumer<String, DataObject> action) {
        if (contents != null) contents.forEach(action);
    }

    public DataObject get(String key) {
        return contents != null ? contents.get(key) : null;
    }

    public <T> void getIfPresent(String key, Class<T> clazz, DataStructApplier<T> applier) {
        var value = get(key);
        if (value != null && clazz.isAssignableFrom(value.getClass())) {
            try {
                applier.apply((T) value);
            } catch (Exception e) {
                throw new ExecutionException("Exception thrown while getting and applying DataStruct value for key \"" + key + "\"", e);
            }
        }
    }

    public <T> T getAs(String key, Class<T> clazz) {
        return getAs(key, clazz, null);
    }

    public <T> T getAs(String key, Class<T> clazz, T defaultValue) {
        var value = get(key);
        if (value != null && clazz.isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        return defaultValue;
    }

    public DataString getAsString(String key) {
        var result = get(key);
        return result instanceof DataString str ? str : result != null ? new DataString(result.toString()) : null;
    }

    public void put(String key, DataObject value) {
        if (contents == null)
            throw new ExecutionException("Can not add item to a NULL Struct: (" + (key != null ? key : "null") + ", " + (value != null ? value : "null") + ")");
        contents.put(key, value);
    }

    public void putIfNotNull(String key, DataObject value) {
        if (value != null) put(key, value);
    }

    public int size() {
        return contents != null ? contents.size() : 0;
    }

    @Override
    public String toString() {
        var schemaName = type.schemaName() + ": ";

        // Return NULL as value
        if (isNull()) return schemaName + "null";

        Iterator<Map.Entry<String, DataObject>> i = entrySet().iterator();

        // Return empty struct as value
        if (!i.hasNext()) return schemaName + "{}";

        StringBuilder sb = new StringBuilder();
        sb.append(schemaName).append('{');
        while (true) {
            Map.Entry<String, DataObject> e = i.next();
            String key = e.getKey();
            DataObject value = e.getValue();
            var quote = (value instanceof DataString ? QUOTE : "");
            sb.append(QUOTE).append(key).append(QUOTE);
            sb.append(':');
            sb.append(quote).append(value == this ? "(this Map)" : value).append(quote);
            if (!i.hasNext())
                return sb.append('}').toString();
            sb.append(',').append(' ');
        }
    }
}
