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
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
import io.axual.ksml.data.value.Null;

/**
 * Represents a wrapper for a null value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataNull} class encapsulates a null value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables null values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataNull extends DataPrimitive<Object> {
    /**
     * Represents the data type of this {@code DataNull}, which is {@code Null},
     * mapped to the schema definition in {@link DataSchemaConstants#NULL_TYPE}.
     * <p>This constant ensures that the type metadata for a {@code DataNull} is
     * consistent across all usages in the framework.</p>
     */
    public static final SimpleType DATATYPE = new SimpleType(Null.class, DataSchemaConstants.NULL_TYPE) {
        @Override
        public ValidationResult checkAssignableFrom(DataType type, ValidationContext context) {
            return this != type ? context.addError("Can only assign \"null\" types to variables of type \"null\"") : context.ok();
        }

        @Override
        public ValidationResult checkAssignableFrom(Object value, ValidationContext context) {
            return value != null ? context.addError("Can only assign \"null\" values to variables of type \"null\"") : context.ok();
        }
    };

    public static final DataNull INSTANCE = new DataNull();

    private DataNull() {
        super(DATATYPE, null);
    }
}
