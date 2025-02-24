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
 * Represents a wrapper for a byte value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataByte} class encapsulates a byte value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables byte values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataByte extends DataPrimitive<Byte> {
    /**
     * Represents the data type of this {@code DataByte}, which is {@code Byte},
     * mapped to the schema definition in {@link DataSchemaConstants#BYTE_TYPE}.
     * <p>This constant ensures that the type metadata for a {@code DataByte} is
     * consistent across all usages in the framework.</p>
     */
    public static final SimpleType DATATYPE = new SimpleType(Byte.class, DataSchemaConstants.BYTE_TYPE);

    /**
     * Constructs a {@code DataByte} instance with a null value.
     * <p>This constructor creates a {@code DataByte} that does not hold any actual
     * {@code Byte} value, effectively representing a "null" byte in the framework.</p>
     */
    public DataByte() {
        this(null);
    }

    /**
     * Constructs a {@code DataByte} instance with the specified {@code Byte} value.
     *
     * <p>If the input value is {@code null}, the {@code DataByte} will represent
     * the absence of a value (a null byte).</p>
     *
     * @param value The {@code Byte} value to encapsulate, or {@code null} to represent a null value.
     */
    public DataByte(Byte value) {
        super(DATATYPE, value);
    }
}
