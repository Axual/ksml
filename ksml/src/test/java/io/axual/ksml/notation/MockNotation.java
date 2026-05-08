package io.axual.ksml.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;

public class MockNotation implements Notation {
    private final String name;
    private final SchemaUsage schemaUsage;
    private final String extension;
    private final SchemaParser schemaParser;

    public MockNotation(String name, SchemaUsage schemaUsage, String extension, SchemaParser schemaParser) {
        this.name = name;
        this.schemaUsage = schemaUsage;
        this.extension = extension;
        this.schemaParser = schemaParser;
    }

    @Override
    public DataType defaultType() {
        return new StructType();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SchemaUsage schemaUsage() {
        return schemaUsage;
    }

    @Override
    public String filenameExtension() {
        return extension;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        return null;
    }

    @Override
    public Converter converter() {
        return null;
    }

    @Override
    public SchemaParser schemaParser() {
        return schemaParser;
    }
}
