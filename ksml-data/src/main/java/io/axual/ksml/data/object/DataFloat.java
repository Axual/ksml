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
import io.axual.ksml.data.type.SimpleType;

/**
 * Represents a wrapper for a float value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataFloat} class encapsulates a float value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables float values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataFloat extends DataPrimitive<Float> {
    /**
     * Represents the data type of this {@code DataFloat}, which is {@code Float},
     * mapped to the schema definition in {@link DataSchemaConstants#FLOAT_TYPE}.
     * <p>This constant ensures that the type metadata for a {@code DataFloat} is
     * consistent across all usages in the framework.</p>
     */
    public static final SimpleType DATATYPE = new SimpleType(Float.class, DataSchemaConstants.FLOAT_TYPE);

    /**
     * Constructs a {@code DataFloat} instance with a null value.
     * <p>This constructor creates a {@code DataFloat} that does not hold any actual
     * {@code Float} value, effectively representing a "null" float in the framework.</p>
     */
    public DataFloat() {
        this(null);
    }

    /**
     * Constructs a {@code DataFloat} instance with the specified {@code Float} value.
     *
     * <p>If the input value is {@code null}, the {@code DataFloat} will represent
     * the absence of a value (a null float).</p>
     *
     * @param value The {@code Float} value to encapsulate, or {@code null} to represent a null value.
     */
    public DataFloat(Float value) {
        super(DATATYPE, value);
    }
}
