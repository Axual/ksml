package io.axual.ksml.data.notation.string;

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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;

public abstract class StringNotation implements Notation {
    private final DataObjectMapper<String> mapper;

    public StringNotation(DataObjectMapper<String> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        return new StringSerde(mapper, type);
    }

    protected RuntimeException noSerdeFor(DataType type) {
        return new ExecutionException(name() + " serde not found for data type: " + type);
    }
}
