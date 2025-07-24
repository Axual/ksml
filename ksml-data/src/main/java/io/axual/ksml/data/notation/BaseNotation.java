package io.axual.ksml.data.notation;

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
import io.axual.ksml.data.type.DataType;
import lombok.Getter;

import java.util.Objects;

@Getter
public abstract class BaseNotation implements Notation {
    private final String name;
    private final String filenameExtension;
    private final DataType defaultType;
    private final Converter converter;
    private final SchemaParser schemaParser;

    protected BaseNotation(String name, String filenameExtension, DataType defaultType, Notation.Converter converter, Notation.SchemaParser schemaParser) {
        Objects.requireNonNull(name);
        this.name = name;
        this.filenameExtension = filenameExtension;
        this.defaultType = defaultType;
        this.converter = converter;
        this.schemaParser = schemaParser;
    }

    protected RuntimeException noSerdeFor(DataType type) {
        return new DataException(name() + " serde not available for data type: " + type);
    }
}
