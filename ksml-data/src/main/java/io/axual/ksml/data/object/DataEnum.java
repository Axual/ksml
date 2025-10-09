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

import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.Flags;

/**
 * {@link DataObject} wrapper for enum-like string values with an associated {@link EnumType}.
 *
 * <p>Two DataEnum instances are equal when both type and value are equal. For convenience,
 * this class also considers equality with a {@link DataString} holding the same string value.</p>
 */
public class DataEnum extends DataPrimitive<String> {
    /**
     * Create a DataEnum.
     *
     * @param type  enum type descriptor
     * @param value enum symbol value
     */
    public DataEnum(EnumType type, String value) {
        super(type, value);
    }

    /**
     * Compares this enum to another object, allowing equality to a DataString with the same value.
     */
    @Override
    public Equal equals(Object other, Flags flags) {
        if (other instanceof DataString dataString && value().equals(dataString.value())) return Equal.ok();
        return super.equals(other, flags);
    }
}
