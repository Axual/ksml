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
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.type.SimpleType;

/**
 * Represents a wrapper for a string value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataString} class encapsulates a string value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables string values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataString extends DataPrimitive<String> {
    /**
     * Represents the data type of this {@code DataString}, which is {@code String},
     * mapped to the schema definition in {@link DataSchemaConstants#STRING_TYPE}.
     * <p>This constant ensures that the type metadata for a {@code DataString} is
     * consistent across all usages in the framework.</p>
     */
    public static final SimpleType DATATYPE = new SimpleType(String.class, DataSchemaConstants.STRING_TYPE);

    /**
     * Constructs a {@code DataString} instance with a null value.
     * <p>This constructor creates a {@code DataString} that does not hold any actual
     * {@code String} value, effectively representing a "null" string in the framework.</p>
     */
    public DataString() {
        this(null);
    }

    /**
     * Constructs a {@code DataString} instance with the specified {@code String} value.
     *
     * <p>If the input value is {@code null}, the {@code DataString} will represent
     * the absence of a value (a null string).</p>
     *
     * @param value The {@code String} value to encapsulate, or {@code null} to represent a null value.
     */
    public DataString(String value) {
        super(DATATYPE, value);
    }

    /**
     * Checks whether the encapsulated {@code String} is empty.
     *
     * @return true if the value is empty, false otherwise.
     */
    public boolean isEmpty() {
        return value() == null || value().isEmpty();
    }

    /**
     * Generate a {@code DataString} instance with the specified {@code String} value.
     *
     * <p>If the input value is {@code null}, the function will return null.</p>
     *
     * @param value The {@code String} value to encapsulate, or {@code null}.
     */
    public static DataString from(String value) {
        return value != null ? new DataString(value) : null;
    }

    @Override
    public Equality equals(Object other, EqualityFlags flags) {
        if (other instanceof String str && str.equals(value())) return Equality.equal();
        if (other instanceof DataEnum enm && enm.value().equals(value())) return Equality.equal();
        return super.equals(other, flags);
    }
}
