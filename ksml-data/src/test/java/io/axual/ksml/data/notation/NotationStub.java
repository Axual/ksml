package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;

/**
 * Minimal configurable {@link Notation} stub for tests. Only the name, filename extension and
 * converter vary between usages; every other method returns {@code null}.
 */
public class NotationStub implements Notation {
    private final String name;
    private final String filenameExtension;
    private final Converter converter;

    public NotationStub(String name, String filenameExtension, Converter converter) {
        this.name = name;
        this.filenameExtension = filenameExtension;
        this.converter = converter;
    }

    @Override
    public SchemaUsage schemaUsage() {
        return null;
    }

    @Override
    public DataType defaultType() {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String filenameExtension() {
        return filenameExtension;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        return null;
    }

    @Override
    public Converter converter() {
        return converter;
    }

    @Override
    public SchemaParser schemaParser() {
        return null;
    }
}
