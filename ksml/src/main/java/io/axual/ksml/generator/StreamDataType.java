package io.axual.ksml.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.common.serialization.Serde;

import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.schema.SchemaUtil;

public class StreamDataType {
    private final DataType type;
    private final Notation notation;
    private final boolean isKey;

    public StreamDataType(DataType type, Notation notation, boolean isKey) {
        this.type = cookType(type);
        this.notation = notation;
        this.isKey = isKey;
    }

    private static DataType cookType(DataType type) {
        // When we get a WindowedType, we automatically convert it into a record type using
        // fixed fields. This allows for processing downstream, since the WindowType itself
        // is KafkaStreams internal and thus not usable in user functions.
        return type instanceof WindowedType
                ? UserRecord.typeOf(SchemaUtil.windowTypeToSchema((WindowedType) type)).type()
                : type;
    }

    public DataType type() {
        return type;
    }

    public Notation notation() {
        return notation;
    }

    public boolean isAssignableFrom(StreamDataType other) {
        return type.isAssignableFrom(other.type);
    }

    @Override
    public String toString() {
        return (notation != null ? notation.name() : "unknown notation") + ":" + type.schemaName();
    }

    public Serde<Object> getSerde() {
        return notation.getSerde(type, isKey);
    }
}
