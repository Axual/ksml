package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import io.axual.ksml.data.type.StructType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.StructSchema;

public class DataStruct extends TreeMap<String, DataObject> implements DataObject {
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

    private final transient StructType type;

    public DataStruct() {
        this(null);
    }

    public DataStruct(StructSchema schema) {
        super(COMPARATOR);
        type = new StructType(schema);
    }

    public void putIfNotNull(String key, DataObject value) {
        if (value != null) put(key, value);
    }

    public void getIfPresent(String key, DataStructApplier<DataObject> applier) {
        getIfPresent(key, DataObject.class, applier);
    }

    public <T> void getIfPresent(String key, Class<T> clazz, DataStructApplier<T> applier) {
        var value = get(key);
        if (value != null && clazz.isAssignableFrom(value.getClass())) {
            try {
                applier.apply((T) value);
            } catch (Exception e) {
                throw new KSMLExecutionException("Exception thrown while getting and applying DataStruct value for key \"" + key + "\"", e);
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

    @Override
    public StructType type() {
        return type;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof DataStruct)) return false;
        return type.equals(((DataStruct) other).type);
    }

    @Override
    public int hashCode() {
        return type.hashCode() + super.hashCode() * 31;
    }

    @Override
    public String toString() {
        Iterator<Map.Entry<String, DataObject>> i = entrySet().iterator();
        if (!i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (; ; ) {
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
