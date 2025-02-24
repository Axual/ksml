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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.util.ListUtil;

/**
 * Represents a wrapper for an enumerated value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataEnum} class encapsulates an enumerated value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables enumerated values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataEnum extends DataPrimitive<String> {
    /**
     * Constructs a {@code DataEnum} instance with the specified {@code String} value.
     *
     * <p>The input value can not be {@code null} and should be one of the allowed symbols
     * from the specified enum type.</p>
     *
     * @param value The {@code String} value to encapsulate, which should be a symbol of the specified {@code EnumType}.
     * @param type  The specified enumeration type, which contains a list of symbols that the enumeration can hold.
     */
    public DataEnum(String value, EnumType type) {
        super(type, value);
        if (!validateValue(value)) {
            throw new DataException("Invalid enum value for type " + type.name() + ": " + value);
        }
    }

    /**
     * Validate that a given value is allowed by the enum type by looking up the value in the symbol list.
     *
     * @return true if the value is found in the symbol list, false otherwise.
     */
    private boolean validateValue(String value) {
        if (value == null) return true;
        final var symbols = ((EnumType) type()).symbols();
        return ListUtil.find(symbols, s -> s.name().equals(value)) != null;
    }
}
