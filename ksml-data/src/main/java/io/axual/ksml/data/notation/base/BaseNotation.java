package io.axual.ksml.data.notation.base;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;

/**
 * Base implementation for Notation that stores common context and helpers.
 * Subclasses specialize in serialization, conversion, and schema parsing behavior.
 * <p>
 * Type-constant properties (notation name, filename extension, schema usage, default type,
 * converter, schema parser) are provided by subclasses via method overrides rather than
 * constructor arguments, since they are fixed per leaf type.
 */
@Getter
public abstract class BaseNotation implements Notation {
    private final NotationContext context;

    protected BaseNotation(NotationContext context) {
        this.context = context != null ? context : new NotationContext();
    }

    /**
     * Returns the base notation name (e.g. "avro", "json", "csv").
     * This is used by the default {@link #name()} implementation and can be
     * combined with a vendor prefix by subclasses like VendorNotation.
     *
     * @return the base notation name
     */
    public abstract String notationName();

    @Override
    public String name() {
        return notationName();
    }

    protected RuntimeException noSerdeFor(DataType type) {
        return new DataException(name() + " serde not available for data type: " + type);
    }
}
